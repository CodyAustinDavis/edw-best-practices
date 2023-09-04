import json
import sqlparse
from sql_metadata import Parser
import requests
import re
import os
from datetime import datetime, timedelta
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, max
from pyspark.sql.types import *


### Helps Materialize temp tables during ETL pipelines
class DeltaHelpers():

    
    def __init__(self, db_name="delta_temp", temp_root_path="dbfs:/delta_temp_db"):
        
        self.spark = SparkSession.getActiveSession()
        self.db_name = db_name
        self.temp_root_path = temp_root_path

        self.dbutils = None
      
        #if self.spark.conf.get("spark.databricks.service.client.enabled") == "true":
        try:     
            from pyspark.dbutils import DBUtils
            self.dbutils = DBUtils(self.spark)
        
        except:
            
            import IPython
            self.dbutils = IPython.get_ipython().user_ns["dbutils"]

        self.session_id =self.dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        self.temp_env = self.temp_root_path + self.session_id
        self.spark.sql(f"""DROP DATABASE IF EXISTS {self.db_name} CASCADE;""")
        self.spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.db_name} LOCATION '{self.temp_env}'; """)
        print(f"Initializing Root Temp Environment: {self.db_name} at {self.temp_env}")
        
        return
    

    def createOrReplaceTempDeltaTable(self, df, table_name):
        
        tblObj = {}
        new_table_id = table_name
        write_path = self.temp_env + new_table_id
        
        self.spark.sql(f"DROP TABLE IF EXISTS {self.db_name}.{new_table_id}")
        self.dbutils.fs.rm(write_path, recurse=True)
        
        df.write.format("delta").mode("overwrite").option("path", write_path).saveAsTable(f"{self.db_name}.{new_table_id}")
        
        persisted_df = self.spark.read.format("delta").load(write_path)
        return persisted_df
 
    def appendToTempDeltaTable(self, df, table_name):
        
        tblObj = {}
        new_table_id = table_name
        write_path = self.temp_env + new_table_id
        
        df.write.format("delta").mode("append").option("path", write_path).saveAsTable(f"{self.db_name}.{new_table_id}")
        
        persisted_df = self.spark.read.format("delta").load(write_path)
        return persisted_df
      
    def removeTempDeltaTable(self, table_name):
        
        table_path = self.temp_env + table_name
        self.dbutils.fs.rm(table_path, recurse=True)
        self.spark.sql(f"""DROP TABLE IF EXISTS {self.db_name}.{table_name}""")
        
        print(f"Temp Table: {table_name} has been deleted.")
        return
    
    def removeAllTempTablesForSession(self):
        
        self.dbutils.fs.rm(self.temp_env, recurse=True)
        ##spark.sql(f"""DROP DATABASE IF EXISTS {self.db_name} CASCADE""") This temp db name COULD be global, never delete without separate method
        print(f"All temp tables in the session have been removed: {self.temp_env}")
        return
        


class SchemaHelpers():
    
    def __init__():
        import json
        return
    
    @staticmethod
    def getDDLString(structObj):
        import json
        ddl = []
        for c in json.loads(structObj.json()).get("fields"):

            name = c.get("name")
            dType = c.get("type")
            ddl.append(f"{name}::{dType} AS {name}")

        final_ddl = ", ".join(ddl)
        return final_ddl
    
    @staticmethod
    def getDDLList(structObj):
        import json
        ddl = []
        for c in json.loads(structObj.json()).get("fields"):

            name = c.get("name")
            dType = c.get("type")
            ddl.append(f"{name}::{dType} AS {name}")

        return ddl
    
    @staticmethod
    def getFlattenedSqlExprFromValueColumn(structObj):
        import json
        ddl = []
        for c in json.loads(structObj.json()).get("fields"):

            name = c.get("name")
            dType = c.get("type")
            ddl.append(f"value:{name}::{dType} AS {name}")

        return ddl
      
 
 ### Class to help easily manage multi statement transactions in Delta. 
 ### ONLY SUPPORTS ONE CONCURRENT WRITING PIPELINE, THIS CAN INVALIDATE OTHER WRITERS DURING A TRANSACTION
 #### OLD: UPDATE: 9/1/2023: This is the very simple version, the other version is now in its own file with more functionality 
class Transaction():
  
  def __init__(self):
    
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
  
  
  def get_starting_snapshot_for_sql(sql_string, tables_in_transaction=[]):
  
    ### This rolls back ALL tables in a transaction to the starting version at the beginning of the transaction, whether or not it was altered. 
    # USE AT OWN RISK AND ENSURE THERE IS ONLY 1 WRITER PER TABLE

    ## Get all statements in MST
    parsed_string = (sql_string
                   .replace("COPY", "INSERT")
                   .replace("MERGE", "INSERT")
                   .replace("TRUNCATE TABLE", "INSERT INTO")
                   .replace("ANALYZE TABLE", "SELECT 1 FROM")
                   .replace("COMPUTE STATISTICS FOR ALL COLUMNS", "") ## Add other variations if you want, deal with OPTIMIZE / Z ORDER later
                   .split(";") ## Get all tables in statement
                  )

    ## Clean tables
    all_tables_in_transaction = [Parser(parsed_string[i]).tables for i,v in enumerate(parsed_string)]
    flattened_tables_in_transaction = [i for x in all_tables_in_transaction for i in x]
    ## Remove raw file read statements in FROM
    p = re.compile('[^/|^dbfs:/|^s3a://|^s3://]')


    flattened_tables_in_transaction = set(flattened_tables_in_transaction + tables_in_transaction)
    ## Get Final Clean Tables
    cleaned_tables_in_transaction = list(set([ s for s in flattened_tables_in_transaction if (p.match(s) or s in tables_in_transaction)]))

    ### Use Private function to get snapshot
    self.get_starting_snapshot_for_table_list(cleaned_tables_in_transaction)
    
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

  
  ### Execute multi statment SQL
  def execute_sql_transaction(self, sql_string):
    ## You do not NEED to run SQL this way to rollback a transaction,
    ## but it automatically breaks up multiple statements in one SQL file into a series of spark.sql() commands
    stmts = sql_string.split(";")
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
      
      
  ### Start a Transaction
  def begin_transaction(self, tables_to_snapshot=[]):
    
    ## Separting into separate statement in case we want to add functionality later (like automated sql parsing to automatically get tables to monitor)
    
    self.get_starting_snapshot_for_table_list(tables_in_transaction=tables_to_snapshot)
    print(f"Starting transaction {self.transaction_id} and monitoring the following tables to rollback on failure: {tables_to_snapshot}")
    
    return
    
    
    
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
       
      
      
      
class DeltaMergeHelpers():
 
    def __init__(self):
        return
 
    @staticmethod
    def retrySqlStatement(spark, operationName, sqlStatement, maxRetries = 10, maxSecondsBetweenAttempts=60):
 
        import time
        maxRetries = maxRetries
        numRetries = 0
        maxWaitTime = maxSecondsBetweenAttempts
        ### Does not check for existence, ensure that happens before merge
 
        while numRetries <= maxRetries:
 
            try: 
 
                print(f"SQL Statement Attempt for {operationName} #{numRetries + 1}...")
 
                spark.sql(sqlStatement)
 
                print(f"SQL Statement Attempt for {operationName} #{numRetries + 1} Successful!")
                break
 
            except Exception as e:
                error_msg = str(e)
 
                print(f"Failed SQL Statment Attmpet for {operationName} #{numRetries} with error: {error_msg}")
 
                numRetries += 1
                if numRetries > maxRetries:
                    break
 
            waitTime = waitTime = 2**(numRetries-1) ## Wait longer up to max wait time for failed operations
 
            if waitTime > maxWaitTime:
                waitTime = maxWaitTime
 
            print(f"Waiting {waitTime} seconds before next attempt on {operationName}...")
            time.sleep(waitTime)