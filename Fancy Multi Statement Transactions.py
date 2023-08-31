# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## TO DO: 
# MAGIC
# MAGIC 1. Add ALL table rule
# MAGIC 2. Add Manual Table Rule
# MAGIC 3. Continue to add edge cases on affected tables: RESTORE TABLE, OPTIMIZE

# COMMAND ----------

import json
import requests
import re
import os
from datetime import datetime, timedelta
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, max
from pyspark.sql.types import *
from sqlglot import parse_one, exp, parse


# COMMAND ----------

# MAGIC %pip install -r helperfunctions/requirements.txt

# COMMAND ----------


 ### Class to help easily manage multi statement transactions in Delta. 
 ### ONLY SUPPORTS ONE CONCURRENT WRITING PIPELINE, THIS CAN INVALIDATE OTHER WRITERS DURING A TRANSACTION
import warnings




class AlteredTableParser():

  """
  Description: this class takes in a chain of sql commands (str seprated by ;) and identifies tables in the chain. By default it looks only for tables that would be altered in a sql statement such as an INSERT/MERGE/DROP/TRUNCATE/COPY/RESTORE/OPTIMIZE, etc. 

  This is for the purpose of identifying which tables need to be snapshotted and rollback during a series of sql statements (transaction)
  
  Steps: 
    
    1. Split multi statement command into single SQL commands
    2. Split into USE sessions and determinate CATALOG/DATABASE scope chain
    3. Clean Custom DBX commands so that we can extract tables well enough (like COPY INTO which is not standard SQL)
    4. For each USE session scope, extract table names, if not fully qualified, fill in the blanks from USE session info
    5. Identify whihc tables are part of READS or WRITES/ALTER/TRUNCATE/MERGE/UPDATE/DELETE/DROP/RESTORE statements
    6. Register full qualified extracted tables with Transaction manager only when transaction mode is "inferred_altered_tables"/"all_tables_in_query"


  """
  def __init__(self, uc_default=False):
    self.uc_default = uc_default

    ## flag: uc_default True/False: hive_metastore/main
    if self.uc_default == True:
      self.default_catalog = "main"
    else:
      self.default_catalog = "hive_metastore"

    self.default_db = "default"

    self.use_sessions = []
    self.clean_tables_to_track = []

  ##### Functions

  ## Just finds tables in any SQL expression
  @staticmethod
  def extract_tables_from_sql_statement(sql_statement):

    if isinstance(sql_statement, str):
      sql_statement = parse_one(sql_statement.strip(), dialect="databricks")
    else:
      pass

    table_list = []
    # find all tables (x, y, z)
    for table in sql_statement.find_all(exp.Table):
      
        ## If reference is not a table and is instead a file path
        if "/" in table.name:
          pass
        else:
          table_list.append({"table": table.name, "schema": table.db, "catalog": table.catalog})

    return table_list


  ## Looks for tables in a specific location of the tree - affected tables from INSERT/MERGE, etc.
  @staticmethod
  def find_affected_tables_in_operation_type(sql_statement, sql_glot_exp_type):

    if isinstance(sql_statement, str):
      sql_statement = parse_one(sql_statement.strip(), dialect="databricks")
    else:
      pass


    found_tables = []

    for st in sql_statement.find_all((sql_glot_exp_type)):
      ## If t.depth = 1, that means it is in the operation and the target table, 0 depth is the command, test on ALL alterable types
      for t in st.find_all((exp.Table)):
        if t.depth == 1:
          found_tables.append({"table": t.name, "schema": t.db, "catalog": t.catalog})
    return found_tables


  ## Goes through all table altering operations and tries to find tables that are alerted within the SQL statement
  @staticmethod
  def find_tables_altered_in_sql(sql_statement):
      
    if isinstance(sql_statement, str):
      sql_statement = parse_one(sql_statement.strip(), dialect="databricks")
    else:
      pass

    r = []

    merge_tables = AlteredTableParser.find_affected_tables_in_operation_type(sql_statement, exp.Merge)
    if len(merge_tables) > 0:
      r.append(merge_tables)

    insert_tables = AlteredTableParser.find_affected_tables_in_operation_type(sql_statement, exp.Insert)
    if len(insert_tables) > 0:
      r.append(insert_tables)

    delete_tables = AlteredTableParser.find_affected_tables_in_operation_type(sql_statement, exp.Delete)
    if len(delete_tables) > 0:
      r.append(delete_tables)

    drop_tables = AlteredTableParser.find_affected_tables_in_operation_type(sql_statement, exp.Drop)
    if len(drop_tables) > 0:
      r.append(drop_tables)

    update_tables = AlteredTableParser.find_affected_tables_in_operation_type(sql_statement, exp.Update)
    if len(update_tables) > 0:
      r.append(update_tables)
    ## TO DO: Add more compelte altering table expressions 
    return r



  ### Scrub and change sql statement to recognize tables from custom statements like COPY INTO / RESTORE / Other unrecognized changing statements
  @staticmethod
  def scrub_for_custom_sql(start_str):

    custom_statement_scrubbing_rules = {}
    custom_statement_scrubbing_rules["COPY"] = {"input": "copy\s", "output": "insert "}
    custom_statement_scrubbing_rules["TRUNCATE"] = {"input": "truncate\s", "output": "drop "}
    custom_statement_scrubbing_rules["OPTIMIZE"] = {"input": "optimize\s", "output": "drop table "} # this doesnt actually change the statement, just which tables it finds
    custom_statement_scrubbing_rules["FILEFORMAT"] = {"input": '\s*fileformat\s*=\s*[a-z]+\s', "output": ""}
    custom_statement_scrubbing_rules["COPY_OPTIONS"] = {"input": "copy_options\((?<=\()(.*?)(?=\))\)", "output": ""}
    ## add more custom rules here as needed


    end_str = None

    for i, j in enumerate(custom_statement_scrubbing_rules):
      input_rule = custom_statement_scrubbing_rules[j].get("input")

      output_str = custom_statement_scrubbing_rules[j].get("output")

      if end_str is None:

        end_str = re.sub(input_rule, output_str, start_str.lower())
      else:
        end_str = re.sub(input_rule, output_str, end_str.lower())

    if end_str is None:
      end_str = start_str

    return str(end_str)


  ##########


  def parse_sql_chain_for_altered_tables(self, statement_chain):

    if isinstance(statement_chain, list):
      pass
    else:
      statement_chain = [i.strip() for i in statement_chain.split(";") if len(i)>0]

    ## Separate Context by USE commands and NOT USE commands
    ## Processing in same order as the SQL commands, so sessions will cascade down
    ## For each use command, we update our default understand of our catalog/db scope
    self.clean_tables_to_track = []
    running_catalog = self.default_catalog
    running_db = self.default_db

    #print(f"Statements in chain = {len(statement_chain)}")
    ### Parse Statemnt Chain
    for i, j in enumerate(statement_chain):

      j = j.strip()

      #print(f"Statement {i} statement {j}")

      is_use = re.search('use\s', j.lower())#.contains("*use")
      is_use_db = re.search('use\s(database|schema)\s', j.lower())#.contains("*use")
      is_use_cat = re.search('use\scatalog\s', j.lower())#.contains("*use")

      if is_use_cat is not None:
        running_catalog = [i for i in re.split('use\scatalog\s', j.lower()) if len(i)> 0][0]

      elif is_use_db is not None:

        running_db_schema = [i for i in re.split('use\sschema\s', j.lower()) if len(i)> 0][0]
        if running_db_schema is None:
          running_db = [i for i in re.split('use\sdatabase\s', j.lower()) if len(i)> 0][0]
        else:
          running_db = running_db_schema
        
      elif is_use is not None:
        cc = [i for i in re.split('use\s', j.lower()) if len(i)> 0][0].split(".")
        if len(cc) > 1:
          running_catalog = cc[0]
          running_db = cc[1]
        else:
          running_db = cc[0]


      #### Session Creation / Organization 

      ## if this use statement happens, create a new session
      if is_use is not None or is_use_db is not None or is_use_cat is not None:
        self.use_sessions.append({"session_use_command": j, "session_cat": running_catalog, "session_db": running_db, "sql_statements": [{"statement": None, "found_tables":[]}]})
      ## Add to most recent session
      else: 
        ### For NON use session statements - All other SQL, clean and parse for altered tables
        try:
          scrubbed_statement = self.scrub_for_custom_sql(j)
        except Exception as e:
          ## TO DO: Add better error handling for end statements
          #print(f"Unable to scrub statement: {j} \n Moving to next...")
          pass

        ## if altered table mode
        try:
          found_tables = self.find_tables_altered_in_sql(scrubbed_statement)

          clean_found_tables = [item for row in found_tables for item in row]

          for i in clean_found_tables:
            t = i.get("table")
            c = i.get("catalog")
            d = i.get("schema")

            if len(c) == 0:
              c = running_catalog
            if len(d) == 0:
              d = running_db

            self.clean_tables_to_track.append(f"{c}.{d}.{t}")


          self.use_sessions[-1]["sql_statements"].append({"statement": j, "found_tables":clean_found_tables})

        except Exception as e:
          ## TO DO: Add better exception handling for end blocks
          ##print(f"Unable to parse statement: {j} \n Moving to next...{str(e)}")
          pass

        ## if all table mode

        ## if manual table mode, just look for the tracked tables (fully qualified)


    return list(set(self.clean_tables_to_track))




class TransactionException(Exception):
  def __init__(self, message, errors):            
      super().__init__(message)
          
      self.errors = errors



class Transaction():
  
  def __init__(self, mode="selected_tables", uc_default=False):
    
    self.available_transaction_modes = ["selected_tables", "inferred_altered_tables", "inferred_all_tables"]

    if mode not in self.available_transaction_modes:
      raise(TransactionException(message=f"Unsupported Transaction Mode, please select valid mode from list: {self.available_transaction_modes}", errors="Unsupported Transaction Mode"))
    
    self.mode = mode
    self.uc_default = uc_default
    self.transaction_id = str(uuid.uuid4())
    self.transaction_start_time = datetime.now()
    self.tables_to_snapshot = []
    self.transaction_snapshot = {str(self.transaction_id):{}}
    self.spark = SparkSession.getActiveSession()

  
  ### private function to get snapshot of delta tables for requested tables
  def get_starting_snapshot_for_table_list(self, tables_in_transaction=[]):
   
    ## This gets the starting version for specific tables and saves the version snapshots at the beginning of the transaction initiation
    # USE AT OWN RISK AND ENSURE THERE IS ONLY 1 WRITER PER TABLE

    ## Transaction Start Time
    transaction_start_time = datetime.now()
    print(f"Transaction Id: {self.transaction_id} with Transaction Start Time: {datetime.now()}")

    transaction_snapshot = {str(self.transaction_id): {"transaction_start_time":transaction_start_time, "snap_shot":{}}}

    flattened_tables_in_transaction = set(tables_in_transaction)
    ## Get Final Clean Tables
    cleaned_tables_in_transaction = list(set([ s for s in flattened_tables_in_transaction if (s in tables_in_transaction)]))
    self.tables_to_snapshot = cleaned_tables_in_transaction
    ## Get starting version

    starting_versions = {}

    for i in cleaned_tables_in_transaction:

      ## During the transaction -- other versions can be added, so you want most recent version IF fails before this specific write attempt of this job
      latest_version = self.spark.sql(f"""DESCRIBE HISTORY {i}""").agg(max(col("version"))).collect()[0][0]

      starting_versions[i] = {"starting_version":latest_version}
      print(f"Starting Version: {i} at version {latest_version}")

    transaction_snapshot.get(self.transaction_id)["snap_shot"] = starting_versions

    self.transaction_snapshot = transaction_snapshot

    return
  
  
  def update_existing_snapshot(self):
    tbls = self.tables_to_snapshot
    self.get_starting_snapshot_for_table_list(tables_in_transaction=tbls)
    print(f"Transaction Commited and Snapshot updated!")
    
    return
  

  ### Some helper getters 
  def get_transaction_id(self):
    return self.transaction_id
  
  
  def get_transaction_snapshot(self):
    return self.transaction_snapshot
  
  
  def get_monitored_tables(self):
    
    tbls = []
    try:
      tbls = list(self.transaction_snapshot.get(self.transaction_id).get("snap_shot").keys())
    except Exception as e:
      if tbls is None or len(tbls) == 0:
        print(f"No tables to monitor...")

    return tbls

  
  ### Manually - Start a Transaction
  ## Call the 2 below functions derectly if you want to manage the transaction yourself outside of just the SQL statements
  def begin_transaction(self, tables_to_snapshot=[]):
    
    ## Separting into separate statement in case we want to add functionality later (like automated sql parsing to automatically get tables to monitor)
    tables_to_manage = tables_to_snapshot
    print(f"Starting transaction {self.transaction_id} and monitoring the following tables to rollback on failure: {tables_to_manage}")
    try:
      self.get_starting_snapshot_for_table_list(tables_in_transaction=tables_to_manage)
    except:
      raise TransactionException(message=f"Unable to snapshot the tables: {tables_to_manage}")
    return
    
  ## Basically updates the snapshot 
  def commit_transaction(self):
    
    try:
      self.update_existing_snapshot()
    except Exception as e:
      raise(e)
    return
  
  
  ### Rollback a transactions for whatever reason to the versions taken at snapshot
  def rollback_transaction(self):
    
    try: 
      current_snapshot = self.transaction_snapshot.get(self.transaction_id).get("snap_shot")
      
      for i in current_snapshot.keys():
        
        version = current_snapshot.get(i).get('starting_version')
        sql_str = f"""RESTORE TABLE {i} VERSION AS OF {version}"""
        self.spark.sql(sql_str)
        print(f"Restored table {i} to version {version}!")
      
    except Exception as e:
      
      if current_snapshot is None or len(current_snapshot) == 0:
        print(f"No snapshots to rollback to... Please inialize a transaction first with a list of tables to monitor...")
        
      else:
        raise(e)
       

  ### Execute multi statment SQL
  def execute_sql_transaction(self, sql_string, tables_to_manage=[]):
    ## You do not NEED to run SQL this way to rollback a transaction,
    ## but it automatically breaks up multiple statements in one SQL file into a series of spark.sql() commands
    stmts = sql_string.split(";")

    ## Get state of desired tables
    if self.mode == "selected_tables":
      if len(tables_to_manage) == 0:
        raise(TransactionException(message="Mode is 'selected_tables', but no tables are provided...", errors="Mode is 'selected_tables', but no tables are provided..."))
      else:
        self.begin_transaction(tables_to_snapshot = tables_to_manage)

    elif self.mode == "inferred_altered_tables":

      ## Do sql glot stuff

      table_altered_parser = AlteredTableParser(uc_default=self.uc_default)
      inferred_tables_to_manage = table_altered_parser.parse_sql_chain_for_altered_tables(sql_string)

      if len(inferred_tables_to_manage) == 0:
        warnings.warn(str(TransactionException(message="Mode is 'inferred_alterd_tables', but couldnt find tables...", errors="Mode is 'inferred_alterd_tables', but couldnt find tables...")))
      else:
        self.begin_transaction(tables_to_snapshot = inferred_tables_to_manage)

    elif self.mode == "inferred_all_tables":
      ## do sql glot stuff
      warnings.warn("Inferred_all_tables mode is not yet supported... this is risky and needs more tests, and might be a bad idea in general. Pick another mode")
      self.begin_transaction(tables_to_snapshot = [])


    
    ## Run the Transaction Logic
    try:
      
      print(f"Running multi statement SQL transaction now...")
      for i, s in enumerate(stmts):
        if len(s.strip()) == 0 or s is None:
          pass
        
        else: 
          print(f"Running statement {i+1} \n {s}")
          self.spark.sql(s)
          
      print(f"Multi Statement SQL Transaction Successfull! Updating Snapshot")
      self.commit_transaction()
        
    except Exception as e:
      print(f"Failed to run all statements... rolling back...")
      self.rollback_transaction()
      print(f"Rollback successful!")
      
      raise(e)
      
      

# COMMAND ----------

y = Transaction(mode="inferred_altered_tables", uc_default=False)

# COMMAND ----------

sqlString = """
USE CATALOG hive_metastore;

CREATE SCHEMA IF NOT EXISTS iot_dashboard;

USE SCHEMA iot_dashboard;

-- Statement 1 -- the load
COPY INTO iot_dashboard.bronze_sensors
FROM (SELECT 
      id::bigint AS Id,
      device_id::integer AS device_id,
      user_id::integer AS user_id,
      calories_burnt::decimal(10,2) AS calories_burnt, 
      miles_walked::decimal(10,2) AS miles_walked, 
      num_steps::decimal(10,2) AS num_steps, 
      timestamp::timestamp AS timestamp,
      value AS value -- This is a JSON object
FROM "/databricks-datasets/iot-stream/data-device/")
FILEFORMAT = json
COPY_OPTIONS('force'='true') -- 'false' -- process incrementally
--option to be incremental or always load all files
; 

-- Statement 2
MERGE INTO iot_dashboard.silver_sensors AS target
USING (SELECT Id::integer,
              device_id::integer,
              user_id::integer,
              calories_burnt::decimal,
              miles_walked::decimal,
              num_steps::decimal,
              timestamp::timestamp,
              value::string
              FROM iot_dashboard.bronze_sensors) AS source
ON source.Id = target.Id
AND source.user_id = target.user_id
AND source.device_id = target.device_id
WHEN MATCHED THEN UPDATE SET 
  target.calories_burnt = source.calories_burnt,
  target.miles_walked = source.miles_walked,
  target.num_steps = source.num_steps,
  target.timestamp = source.timestamp
WHEN NOT MATCHED THEN INSERT *;

USE iot_dashboard;

-- This calculate table stats for all columns to ensure the optimizer can build the best plan
-- Statement 3

ANALYZE TABLE iot_dashboard.silver_sensors COMPUTE STATISTICS FOR ALL COLUMNS;

-- Statement 4
-- Truncate bronze batch once successfully loaded
TRUNCATE TABLE iot_dashboard.bronze_sensors;
"""

# COMMAND ----------

s = """
MERGE INTO iot_dashboard.silver_sensors AS target
USING (SELECT Id::integer,
              device_id::integer,
              user_id::integer,
              calories_burnt::decimal,
              miles_walked::decimal,
              num_steps::decimal,
              timestamp::timestamp,
              value::string
              FROM iot_dashboard.bronze_sensors) AS source
ON source.Id = target.Id
AND source.user_id = target.user_id
AND source.device_id = target.device_id
WHEN MATCHED THEN UPDATE SET 
  target.calories_burnt = source.calories_burnt,
  target.miles_walked = source.miles_walked,
  target.num_steps = source.num_steps,
  target.timestamp = source.timestamp
WHEN NOT MATCHED THEN INSERT *;

OPTIMIZE iot_dashboard.gold ZORDER BY (id);

"""

# COMMAND ----------

# DBTITLE 1,Initialize Transaction Class
x = Transaction(mode="inferred_altered_tables", uc_default=False)

# COMMAND ----------

# DBTITLE 1,Execute a multi statement SQL transaction from a SQL string
## This method is great because to do not need to rollback manually, it is handled for you
## This statement auto-commmits on success. If you do not want that, you can write pyspark or regular SQL outside of this method and then manually rollback
x.execute_sql_transaction(sqlString)

# COMMAND ----------

x.get_transaction_snapshot()

# COMMAND ----------

# DBTITLE 1,Manually rollback a transaction from most recent explicit snapshot for tables
### If you use the SQL execute method, it auto commits!! So you cannot roll back once it succeed. It will do it automatically. You can still use all the manual methods if you want to opt out of auto handling the rollback/committ process
x.rollback_transaction()
