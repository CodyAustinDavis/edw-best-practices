# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## This notebook uses the query history API on Databricks SQL to pull the query history for all users within the last X days and builds a SQL profile from the query text to find most key columns to ZORDER on
# MAGIC 
# MAGIC 
# MAGIC ### Depedencies
# MAGIC <li> https://github.com/macbre/sql-metadata -- pip install sql-metadata
# MAGIC <li> Ensure that you either get a token as a secret or use a cluster with the env variable called DBX_TOKEN to authenticate to DBSQL
# MAGIC 
# MAGIC   
# MAGIC DBX_TOKEN = os.environ.get("DBX_TOKEN")

# COMMAND ----------

# MAGIC %pip install sqlparse
# MAGIC %pip install sql-metadata

# COMMAND ----------

import json
import sqlparse
from sql_metadata import Parser
import requests
import re
import os
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

DBX_TOKEN = os.environ.get("DBX_TOKEN")

# COMMAND ----------

# DBTITLE 1,Apply Query Profile Filter

from datetime import datetime, timezone

def ms_timestamp(dt):
    return int(round(dt.replace(tzinfo=timezone.utc).timestamp() * 1000, 0))
 

# COMMAND ----------

# DBTITLE 1,Get Profile Params
dbutils.widgets.dropdown("Query History Lookback Period (days)", defaultValue="3",choices=["1","3","7","14","30","60","90"])
dbutils.widgets.text("SQL Warehouse Ids (csv list)", "")
dbutils.widgets.text("Workspace DNS:", "")

# COMMAND ----------

# DBTITLE 1,Get and clean up params
lookbackPeriod = int(dbutils.widgets.get("Query History Lookback Period (days)"))
warehouseIdsList = [i.strip() for i in dbutils.widgets.get("SQL Warehouse Ids (csv list)").split(",")]
workspaceName = dbutils.widgets.get("Workspace DNS:").strip()

print(f"Loading Query Profile to delta from workspace: {workspaceName} \n from Warehouse Ids: {warehouseIdsList} \n for the last {lookbackPeriod} days...")

# COMMAND ----------

end_timestamp = datetime.now()
start_timestamp = end_timestamp - timedelta(days = lookbackPeriod)

start_ts_ms = ms_timestamp(start_timestamp)
end_ts_ms = ms_timestamp(end_timestamp)
print(f"Getting Query History to parse from period: {start_timestamp} to {end_timestamp}")

# COMMAND ----------

requestString = {
"filter_by": {
  "query_start_time_range": {
"end_time_ms": end_ts_ms,
"start_time_ms": start_ts_ms
},
"statuses": [
"FINISHED"
],
"warehouse_ids": warehouseIdsList
},
"include_metrics": "true",
"max_results": "1000"
}

# COMMAND ----------

## Convert dict to json
v = json.dumps(requestString)

# COMMAND ----------

## This API endpoint is not currently documented publicly
uri = f"https://{workspaceName}/api/2.0/sql/history/queries"
headers_auth = {"Authorization":f"Bearer {DBX_TOKEN}"}

## This file could be large
endp_resp = requests.get(uri,data=v, headers=headers_auth).json()

# COMMAND ----------

initial_resp = endp_resp.get("res")

# COMMAND ----------

next_page = endp_resp.get("next_page_token")
has_next_page = endp_resp.get("has_next_page")

if has_next_page is True:
  print(f"Has next page?: {has_next_page}")
  print(f"Getting next page: {next_page}")

# COMMAND ----------

# DBTITLE 1,Page through results
page_responses = []

while has_next_page is True: 
  
  print(f"Getting results for next page... {next_page}")
  
  raw_page_request = {
  "include_metrics": "true",
  "max_results": 1000,
  "page_token": next_page
  }
  
  json_page_request = json.dumps(raw_page_request)
  
  ## This file could be large
  current_page_resp = requests.get(uri,data=json_page_request, headers=headers_auth).json()
  
  current_page_queries = current_page_resp.get("res")
  
  ## Add Current results to total results or write somewhere (to s3?)
  
  page_responses.append(current_page_queries)
  
  ## Get next page
  next_page = current_page_resp.get("next_page_token")
  has_next_page = current_page_resp.get("has_next_page")
  
  if has_next_page is False:
    break

# COMMAND ----------

# DBTITLE 1,Collect Final Responses and Save to Delta for SQL Tree Parsing
final_responses = initial_resp + page_responses

print(f"Saving {len(final_responses)} Queries To Delta for Profiling")

# COMMAND ----------

all_responses = [x for xs in page_responses for x in xs] + initial_resp

print(f"Saving {len(all_responses)} Queries To Delta for Profiling")

# COMMAND ----------


# notice the parens after the type name
raw_queries_df = spark.createDataFrame(all_responses, StringType())

# COMMAND ----------

raw_queries_df = (spark.createDataFrame(all_responses))

# COMMAND ----------

raw_queries_df.createOrReplaceTempView("raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS delta_optimizer;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE delta_optimizer.raw_query_history_statistics
# MAGIC AS
# MAGIC SELECT
# MAGIC query_id,
# MAGIC query_start_time_ms,
# MAGIC query_end_time_ms,
# MAGIC duration,
# MAGIC query_text,
# MAGIC status,
# MAGIC statement_type,
# MAGIC rows_produced,
# MAGIC metrics
# MAGIC FROM raw
# MAGIC WHERE statement_type = 'SELECT';

# COMMAND ----------

# DBTITLE 1,Calculate Summary Statistics
# MAGIC %sql
# MAGIC 
# MAGIC --Calculate Query Statistics to get importance Rank by Query (duration, rows_returned)
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE delta_optimizer.query_summary_statistics
# MAGIC AS (
# MAGIC   WITH raw_query_stats AS (
# MAGIC     SELECT query_id,
# MAGIC     AVG(duration) AS AverageQueryDuration,
# MAGIC     AVG(rows_produced) AS AverageRowsProduced,
# MAGIC     COUNT(*) AS TotalQueryRuns
# MAGIC     FROM delta_optimizer.raw_query_history_statistics
# MAGIC     WHERE status = 'FINISHED'
# MAGIC     AND statement_type = 'SELECT'
# MAGIC     GROUP BY query_id
# MAGIC   )
# MAGIC   SELECT 
# MAGIC   *,
# MAGIC   ROW_NUMBER() OVER(ORDER BY AverageQueryDuration DESC) AS QueryImportanceRank
# MAGIC   FROM raw_query_stats
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM delta_optimizer.raw_query_history_statistics

# COMMAND ----------

# DBTITLE 1,v1 -DEPRECATED - The hard way - UDF to parse query tree and get list of columns used in joins and filters
import sqlparse
import re
## This function parses a SQL string a returns a list of tables with potential columnsed used to join

## TO DO: add functionality for getting columns used to filter with WHERE/ORDER -- requires aliases or just lots of extra columns

@udf("string")
def getPotentialJoinedColumns(sqlString):
  parsed = sqlparse.parse(sqlString)[0]
  sqlParts = parsed.tokens

  y = sqlparse.sql.IdentifierList(sqlParts)
  z = [i for i in y if len(str(i)) > 2]

  ylist = list([i for i in z])

  table_tree_obj = {}

  ## Get tables for joins
  for i,k in enumerate(ylist):
    ss = str(k)

    if str(k.ttype) == 'Token.Keyword' and (str(k.value).find('FROM') >= 0 or (str(k.value).find('JOIN') >= 0)):

      try:

        table_tree_obj[re.split('[ AS]', str(z[i+1]))[0]] = {"JoinedColumns":[]}
        ## Get comparisons
        comp = z[i+2]

        if str(comp.ttype) == 'Token.Comparison' or (comp.ttype is None):
          try:
            #print(f" There is a Comparison here! at {i+2} --> {z[i+2]}")

            comp_str = str(z[i+2])
            parsed_cols = list(set([(i[i.find(".")+1:]).strip() for i in re.split('[=|<|>|OR|AND]', comp_str) if len(i)>= 1]))

            #print(parsed_cols)
            table_tree_obj[re.split('[ AS]', str(z[i+1]))[0]]["JoinedColumns"] = parsed_cols

          except:
            pass      

      except:
        pass


  return table_tree_obj


# COMMAND ----------

# DBTITLE 1,Parse SQL Tree for filtered columns and table map
@udf("array<string>")
def getParsedFilteredColumnsinSQL(sqlString):
  ## output ["table_name:column_name,table_name:colunmn:name"]
  final_table_map = []
  
  try: 
    results = Parser(sqlString)

    final_columns = []

    ## If just a select, then skip this and just return the table
    try:
      final_columns.append(results.columns_dict.get("where"))
      final_columns.append(results.columns_dict.get("join"))

    except:
      for tbl in results.tables:
        final_table_map.append(f"{tbl}:")
    
    final_columns_clean = [i for i in final_columns if i is not None]
    flatted_final_cols = list(set([x for xs in final_columns_clean for x in xs]))

    ## Try to map columns to specific tables for simplicity downstream

    """Rules -- this isnt a strict process cause we will filter things later, what needs to happen is we need to get all possible columns on a given table, even if not true

    ## Aliases are already parsed so the column is either fully qualified or fully ambiguous
    ## Assign a column to table if: 
    ## 1. column has an explicit alias to it
    ## 2. column is not aliased
    """

    for tbl in results.tables:
      found_cols = []
      for st in flatted_final_cols:

       ## Get Column Part
        try:
          column_val = st[st.rindex('.')+1:] 
        except: 
          column_val = st

        ## Get Table Part
        try:
          table_val = st[:st.rindex('.')] or None
        except:
          table_val = None
          
        ## Logic that add column if tbl name is found or if there was no specific table name for the column
        if st.find(tbl) >= 0 or (table_val is None):
          if column_val is not None and len(column_val) > 1:
            
            final_table_map.append(f"{tbl}:{column_val}")

      #found_cols_clean = [i for i in found_cols if (found_cols is not None and len(i) > 1)]

      #final_table_map.append({"Table": {tbl: {"FilteredColumns": found_cols_clean}}})

    ## Final Results will be a crosswalk of table_name, column_name, # of times used in joins/filters from ALL queries, # of queries used in join/filters
    #parsed_results = {}
  except Exception as e:
    final_table_map = [str(f"ERROR: {str(e)}")]
  
  return final_table_map

# COMMAND ----------

df = spark.sql("""SELECT DISTINCT query_id, query_text 
                  FROM delta_optimizer.raw_query_history_statistics
                  WHERE statement_type = 'SELECT'""")

df.display()

# COMMAND ----------

df_profiled = df.withColumn("profiled_columns", getParsedFilteredColumnsinSQL(F.col("query_text")))

#df_profiled.createOrReplaceTempView("parsed")

spark.sql("""DROP TABLE IF EXISTS delta_optimizer.parsed_distinct_queries""")

df_profiled.write.format("delta").saveAsTable("delta_optimizer.parsed_distinct_queries")

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH exploded_parsed_cols AS (SELECT 
# MAGIC explode(profiled_columns) AS explodedCols,
# MAGIC query_id,
# MAGIC query_text
# MAGIC FROM delta_optimizer.parsed_distinct_queries
# MAGIC ),
# MAGIC 
# MAGIC step2 AS (
# MAGIC SELECT 
# MAGIC split(explodedCols, ":")[0] AS TableName,
# MAGIC split(explodedCols, ":")[1] AS ColumnName,
# MAGIC query_id,
# MAGIC query_text
# MAGIC FROM exploded_parsed_cols),
# MAGIC 
# MAGIC step3 AS (
# MAGIC   SELECT
# MAGIC   TableName,
# MAGIC   ColumnName,
# MAGIC   COUNT(DISTINCT query_id) AS NumberOfQueriesFilteringOnThisColumn
# MAGIC   FROM step2
# MAGIC   GROUP BY TableName, ColumnName
# MAGIC )
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM step3
# MAGIC WHERE TableName LIKE ('codydemos%')
# MAGIC ORDER BY TableName, NumberOfQueriesFilteringOnThisColumn DESC 

# COMMAND ----------

# DBTITLE 1,Calculate Statistics
# MAGIC %sql
# MAGIC 
# MAGIC -- OUTPUT: table with table_name, column_name, # query filter references, # total execution referencing, # avg duration of query when referenced
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE delta_optimizer.query_column_statistics
# MAGIC AS (
# MAGIC   WITH exploded_parsed_cols AS (SELECT 
# MAGIC   explode(profiled_columns) AS explodedCols,
# MAGIC   query_id,
# MAGIC   query_text
# MAGIC   FROM delta_optimizer.parsed_distinct_queries
# MAGIC   )
# MAGIC 
# MAGIC   SELECT 
# MAGIC   split(explodedCols, ":")[0] AS TableName,
# MAGIC   split(explodedCols, ":")[1] AS ColumnName,
# MAGIC   root.query_text,
# MAGIC   hist.*
# MAGIC   FROM exploded_parsed_cols AS root
# MAGIC   LEFT JOIN delta_optimizer.query_summary_statistics AS hist USING (query_id)--SELECT statements only included
# MAGIC )
