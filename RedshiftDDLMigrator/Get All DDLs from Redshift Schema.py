# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Redshift --> Databricks DDL Migrator
# MAGIC 
# MAGIC #### Dependencies: requires redshift connector being installed on the running cluster
# MAGIC 
# MAGIC ### v000 (PROTOTYPE)
# MAGIC ### Author: Cody Austin Davis
# MAGIC ### Date: 8/13/2022

# COMMAND ----------

# MAGIC %pip install sqlparse
# MAGIC %pip install sql-metadata

# COMMAND ----------

from helperfunctions.redshiftchecker import RedshiftChecker
import json
import sqlparse
from sql_metadata import Parser
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Get Auth for Redshift Cluster from secrets
redshift_user = dbutils.secrets.get(scope='redshift', key = 'username')
redshift_password = dbutils.secrets.get(scope='redshift', key = 'password')

# COMMAND ----------

# DBTITLE 1,Load Env Redshift Variables from config or secrets
hostname_redshift = dbutils.secrets.get(scope='redshift', key = 'hostname_redshift')
port_redshift = dbutils.secrets.get(scope='redshift', key = 'port_redshift')
tempdir_redshift_unloads = dbutils.secrets.get(scope='redshift', key = 'temp_dir_redshift_uploads')
iam_role_redshift = dbutils.secrets.get(scope='redshift', key = 'iam_role_redshift')

print(f"Running testing off: {hostname_redshift}")

# COMMAND ----------


redshift_url = f"jdbc:redshift://{hostname_redshift}:{port_redshift}/detectionlive?user={redshift_user}&password={redshift_password}&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"

# COMMAND ----------

redshiftChecker = RedshiftChecker(redshift_url, iam_role_redshift, tempdir_redshift_unloads)

# COMMAND ----------

# DBTITLE 1,Get Raw DDL Statements Over Time
df = redshiftChecker.getRedshiftQueryResult("""SELECT 
LISTAGG(CASE WHEN LEN(RTRIM(text)) = 0 THEN text ELSE RTRIM(text) END) WITHIN GROUP (ORDER BY sequence) as query_statement, xid, endtime
FROM stl_ddltext GROUP BY xid, endtime ORDER BY xid, endtime""")

display(df)

# COMMAND ----------

#df = redshiftChecker.getRedshiftQueryResult("""select * from admin.v_generate_tbl_ddl""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS redshift_migration;
# MAGIC USE redshift_migration;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## To Do: 
# MAGIC 
# MAGIC 1. Translate primary key generation object and automatically run the sync command (stretch goal)
# MAGIC 2. DONE - Add auto sort and dist keys --> ZORDER
# MAGIC 3. Translate default values in DDL statements
# MAGIC 4. Make Identity column generation more robust (translate increments, etc.)
# MAGIC 
# MAGIC 
# MAGIC Output: 
# MAGIC 
# MAGIC {"<table_name": {"ddl": "<sql_string>", "optimize_command": "<dml_string>"},...}
# MAGIC Query text, command Id, rawSql String, run timestamp, recency rank, table_name, clean DDL, clean OPTIMIZE command

# COMMAND ----------

# DBTITLE 1,Parsing Functions
import re

def get_table_name(tokens):
    for token in reversed(tokens):
        if token.ttype is None:
            return token.value
    return ""

## Get zorder cols from DIST and SORT KEYS
def get_zorder_cols(tokens):
    
    zorder_keys = []

    for i, t in enumerate(tokens):

        if re.search('distkey', str(t).lower()):
            dc = str(tokens[i+1])
            dist_cols = [i.strip() for i in re.sub("[\t\n]", "", dc[dc.find("(")+1:dc.find(")")]).split(",")]
            #print(f"found dist key! {dist_cols}")

        if re.search('sortkey', str(t).lower()):
            sc = str(tokens[i+1])
            sort_cols = [i.strip() for i in re.sub("[\t\n]", "", sc[sc.find("(")+1:sc.find(")")]).split(",")]
            #print(f"found sort key! {sort_cols}")

        ## TO DO: Make need to automate the ordering of these cols since they will go into a Z ORDER

    zorder_keys = list(set(dist_cols + sort_cols))
 
    return zorder_keys or []

### See if columns is an identity column or not

def is_identity_column(token):
    has_id_cols = False
    
    if re.search('identity', str(token).lower()):
        dc = str(token)
        has_id_cols = True
        return has_id_cols

    return has_id_cols



## Spark UDF function
@udf("string")
def getDDLFromSQLString(sqlString):

    parse = sqlparse.parse(sqlString)

    ## For each statement in the sql string (can be thousands, parse SQL String and built DDL expression and optimize statement)
    final_ddl_json = {}

    for stmt in parse:
        # Get all the tokens except whitespaces
        tokens = [t for t in sqlparse.sql.TokenList(stmt.tokens) if t.ttype != sqlparse.tokens.Whitespace]
        is_create_stmt = False

        zorder_cols = get_zorder_cols(tokens)

        for i, token in enumerate(tokens):
            # Check if create statement
            if token.match(sqlparse.tokens.DDL, 'CREATE'):
                is_create_stmt = True
                continue


            # If it was a create statement and the current token starts with "("
            if is_create_stmt and token.value.startswith("("):
                # Get the table name by looking at the tokens in reverse order till you find
                # a token with None type

                ## Get Table Info 
                table_name = get_table_name(tokens[:i])
                #print (f"table: {table_name}")

                ### Get Column Info
                #txt = token.value

                ## Split on comma but only if not in parentheses (eg. NUMERIC(10,2))
                s = txt[1:txt.rfind(")")].replace("\n","")
                columns = re.split(r',\s*(?![^()]*\))', s)

                ## Prep for rebuilding SQL String
                target_ddl_array = []

                for column in columns:
                    c = ' '.join(column.split()).split()
                    c_name = c[0].replace('\"',"")
                    c_type = c[1]  # For condensed type information 

                    c_type_full = " ".join(c[1:]) # For detailed type information ## Do not do this for stage 1 of migrator
                    ## Check for identity generation column
                    is_id = is_identity_column(c_type_full)

                    ## Make identity column if id col found in Redshift
                    ## !!! USER MUST RUN ID SYNC WHEN MOVING ACTUAL EXISTING IDS ON FIRST BACKFILL FROM REDSHIFT!!!
                    if is_id is True:
                        c_type = c_type + " GENERATED BY DEFAULT AS IDENTITY"

                    #print (f"column: {c_name}")
                    #print (f"date type: {c_type}")

                    ## Rebuild String for DBX
                    clean_col = c_name + " " + c_type
                    target_ddl_array.append(clean_col)

                #print(f"Table columns: {target_ddl_array}")
                #print(f"Z ORDER Columns: {zorder_cols}")

                ## Build entire statement
                full_ddl_string = f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join(target_ddl_array)});"
                full_optimize_string = f"OPTIMIZE {table_name} ZORDER BY ({','.join(zorder_cols)});"
                
                #print(full_ddl_string)
                #print(full_optimize_string)
                #print ("---"*20)

                final_ddl_json[table_name] = {"ddl": full_ddl_string, "optimize_command": full_optimize_string}

                break
                
    return final_ddl_json

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## TO DO: 
# MAGIC 
# MAGIC 1. Pull out database and table from results
# MAGIC 2. Get most recent DDL statement for each table
# MAGIC 3. Write command to auto run commands and migrate entire DDL in 1 command
# MAGIC 4. Make incremental and merge results into target table

# COMMAND ----------

(df.withColumn("ParsedDDLAndOptimizeCommands", getDDLFromSQLString(col("query_statement")))
 ## Get most recent table ddl command
 ## merge into target table (just truncating and reloading right now)
 ## Add separate command to run all statements
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("redshift_migration.redshift_ddl_to_databricks")
)
