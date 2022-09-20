# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Redshift --> Databricks DDL Migrator
# MAGIC 
# MAGIC ### v000 (PROTOTYPE)
# MAGIC ### Author: Cody Austin Davis
# MAGIC ### Date: 8/13/2022
# MAGIC 
# MAGIC 
# MAGIC #### DEPENDENCIES: 
# MAGIC 
# MAGIC <li> 1. Must first create a table/view in Redshift that contains all historical DDL statements. This statement can be found from AWS <a href="https://github.com/awslabs/amazon-redshift-utils/blob/master/src/AdminViews/v_generate_tbl_ddl.sql">here </a>. You can name the table whatever you would like and supply the table name in this notebook.
# MAGIC <li> 2. Must install the Redshift <> Databricks Jar file to the cluster on Databricks found here: <a href="https://docs.databricks.com/data/data-sources/aws/amazon-redshift.html" >Amazon Redshift Connector</a>
# MAGIC   
# MAGIC   
# MAGIC #### ROADMAP: 
# MAGIC   
# MAGIC <li> CALL OUT EDGE CASES: Super data type, timeszones
# MAGIC <li> Parse external tables - from Redshift
# MAGIC <li> Edge data type (timezone, encoding, etc.)
# MAGIC <li> Translate primary key generation object and automatically run the sync command (stretch goal)
# MAGIC <li> Translate default values in DDL statements
# MAGIC <li> Make Identity column generation more robust (translate increments, etc.)

# COMMAND ----------

# MAGIC %pip install sqlparse
# MAGIC %pip install sql-metadata

# COMMAND ----------

import json
import sqlparse
from sql_metadata import Parser
from pyspark.sql.functions import *

# COMMAND ----------

redshift_user = dbutils.secrets.get(scope='rm_redshift', key = 'username') ## Supply your own secret values or raw keys here for username and password
redshift_password = dbutils.secrets.get(scope='rm_redshift', key = 'password')

# COMMAND ----------

hostname_redshift = '<insert-host-name>'
port_redshift = '5439'
tempdir_redshift_unloads = '<insert-temp-s3-path>'
iam_role_redshift = '<insert-redshift-iam-role>'
database = "<insert-detection-database>"
print(f"Running testing off: {hostname_redshift}")

# COMMAND ----------

# DBTITLE 1,Get DDL Admin View if not exists
dbutils.widgets.text("Redshift DDL Table Name", "")
redshift_table_name = dbutils.widgets.get("Redshift DDL Table Name")


dbutils.widgets.text("Redshift Schemas to migrate(csv)", "")
schemas_to_migrate = [i.strip() for i in dbutils.widgets.get("Redshift Schemas to migrate(csv)").split(",") if len(i) > 0]

if len(schemas_to_migrate) == 0:
    schemas_to_migrate = "All"
    
print(f"Extracting DDL from the following table: {redshift_table_name}")
print(f"Migrating the following schemas: {schemas_to_migrate}")


# COMMAND ----------

redshift_url = f"jdbc:redshift://{hostname_redshift}:{port_redshift}/{database}?user={redshift_user}&password={redshift_password}&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory"

# COMMAND ----------

## Pull and Aggregate mode recent DDL statement for all tables, and optionally filter for a set of schemas

rsh_query = f"""SELECT LISTAGG(CASE WHEN LEN(RTRIM(ddl)) = 0 THEN ddl ELSE RTRIM(ddl) END) WITHIN GROUP (ORDER BY seq) as query_statement, schemaname, tablename 
                   FROM {redshift_table_name} GROUP BY schemaname, tablename"""



if schemas_to_migrate == "All":
    view_create = (spark.read.format("com.databricks.spark.redshift")
                   .option("url", redshift_url)
                   .option("query", rsh_query)
                   .option("tempdir", tempdir_redshift_unloads)
                   .option("aws_iam_role", iam_role_redshift)
                   .load()
                  )
else: 
    view_create = (spark.read.format("com.databricks.spark.redshift")
                   .option("url", redshift_url)
                   .option("query", rsh_query)
                   .option("tempdir", tempdir_redshift_unloads)
                   .option("aws_iam_role", iam_role_redshift)
                   .load()
                   .filter(col("schemaname").isin(*schemas_to_migrate))
                  )   

# COMMAND ----------

spark.sql("""CREATE DATABASE IF NOT EXISTS redshift_migration;""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC Output: 
# MAGIC 
# MAGIC {"<table_name": {"ddl": "<sql_string>", "optimize_command": "<dml_string>"},...}
# MAGIC Query text, command Id, rawSql String, run timestamp, recency rank, table_name, clean DDL, clean OPTIMIZE command

# COMMAND ----------

@udf("string")
def getCreateStatementOnly(sqlString):
    try:
        resultStr = sqlString.partition("CREATE")[1] + sqlString.partition("CREATE")[2].partition(";")[0]
        return resultStr
    except:
        resultStr = ''
        return resultStr
    
    

def getCreateStatementOnlyPython(sqlString):
    try:
        resultStr = sqlString.partition("CREATE")[1] + sqlString.partition("CREATE")[2].partition(";")[0]
        return resultStr
    except:
        resultStr = ''
        return resultStr

# COMMAND ----------

# DBTITLE 1,Parsing Functions
import re

def get_table_name(tokens):
    for token in reversed(tokens):
        if token.ttype is None:
            return token.value
    return ""

## Get zorder cols from DIST and SORT KEYS

## Allow ZORDER cols to be empty (no ZORDER, just optimize)
def get_zorder_cols(tokens):
    
    zorder_keys = []
    dist_cols = []
    sort_cols = []
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

    cleanSqlString = getCreateStatementOnlyPython(sqlString)
    parse = sqlparse.parse(cleanSqlString)

    ## For each statement in the sql string (can be thousands, parse SQL String and built DDL expression and optimize statement)
    final_ddl_json = {}

    try:
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
                    txt = token.value

                    ## Split on comma but only if not in parentheses (eg. NUMERIC(10,2))
                    s = txt[1:txt.rfind(")")].replace("\n","")
                    #columns = re.split(r',\s*(?![^()]*\))', s)
                    columns = re.split(r"(?<=[^\d+()]),(?![^()]*\))", s)
                    
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
                            c_type = "BIGINT" + " GENERATED BY DEFAULT AS IDENTITY"

                        #print (f"column: {c_name}")
                        #print (f"date type: {c_type}")

                        ## Rebuild String for DBX
                        clean_col = c_name + " " + c_type
                        
                        if clean_col.lower() == 'primary key':
                            pass
                        else:
                            target_ddl_array.append(clean_col)

                    #print(f"Table columns: {target_ddl_array}")
                    #print(f"Z ORDER Columns: {zorder_cols}")

                    ## Build entire statement
                    full_ddl_string = f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join(target_ddl_array)});"
                    
                    if len(zorder_cols) >= 1:
                        full_optimize_string = f"OPTIMIZE {table_name} ZORDER BY ({','.join(zorder_cols)});"
                    else:
                        full_optimize_string = f"OPTIMIZE {table_name};"

                    #print(full_ddl_string)
                    #print(full_optimize_string)
                    #print ("---"*20)

                    final_ddl_json = {"table_name": table_name, "ddl": full_ddl_string, "optimize_command": full_optimize_string}

                    break
    except:
        pass
                
    return json.dumps(final_ddl_json)

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

(view_create.withColumn("ParsedDDL", getDDLFromSQLString(col("query_statement")))
 ## Get most recent table ddl command
 ## merge into target table (just truncating and reloading right now)
 ## Add separate command to run all statements
 .write
 .format("delta")
 .option("overwriteSchema", "true")
 .mode("overwrite")
 .saveAsTable("redshift_migration.redshift_ddl_to_databricks")
)

# COMMAND ----------

spark.sql("""SELECT query_statement, ParsedDDL:ddl, ParsedDDL:optimize_command FROM redshift_migration.redshift_ddl_to_databricks""")
