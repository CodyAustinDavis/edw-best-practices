# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## This notebook uses the query history API on Databricks SQL to pull the query history for all users within the last X days and builds a SQL profile from the query text to find most key columns to ZORDER on
# MAGIC 
# MAGIC 
# MAGIC ### RETURNS
# MAGIC 
# MAGIC 1. delta_optimizer.query_column_statistics - Column level query stats
# MAGIC 2. delta_optimizer.query_summary_statistics - Query level query stats
# MAGIC 3. delta_optimizer.raw_query_history_statistics - Raw Query History Stats
# MAGIC 
# MAGIC ### Depedencies
# MAGIC <li> https://github.com/macbre/sql-metadata -- pip install sql-metadata
# MAGIC <li> Ensure that you either get a token as a secret or use a cluster with the env variable called DBX_TOKEN to authenticate to DBSQL
# MAGIC 
# MAGIC   
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

# DBTITLE 1,Get Period Params for Query History request
end_timestamp = datetime.now()
start_timestamp = end_timestamp - timedelta(days = lookbackPeriod)

start_ts_ms = ms_timestamp(start_timestamp)
end_ts_ms = ms_timestamp(end_timestamp)
print(f"Getting Query History to parse from period: {start_timestamp} to {end_timestamp}")

# COMMAND ----------

# DBTITLE 1,Build Dynamic Request
requestString = {
    "filter_by": {
      "query_start_time_range": {
      "end_time_ms": end_ts_ms,
        "start_time_ms": start_ts_ms
    },
    "statuses": [
        "FINISHED", "CANCELED"
    ],
    "warehouse_ids": warehouseIdsList
    },
    "include_metrics": "true",
    "max_results": "1000"
}

## Convert dict to json
v = json.dumps(requestString)

print(v)

# COMMAND ----------

# DBTITLE 1,Submit Initial Request
uri = f"https://{workspaceName}/api/2.0/sql/history/queries"
headers_auth = {"Authorization":f"Bearer {DBX_TOKEN}"}

## This file could be large
## Convert response to dict
endp_resp_raw = requests.get(uri,data=v, headers=headers_auth).json()

#print(endp_resp)
initial_resp = endp_resp.get("res")

print(workspaceName)
print(DBX_TOKEN)
print(initial_resp)

# COMMAND ----------

# DBTITLE 1,Check if we need to page through results
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
all_responses = [x for xs in page_responses for x in xs] + initial_resp

print(f"Saving {len(all_responses)} Queries To Delta for Profiling")

# COMMAND ----------

# DBTITLE 1,Create Data Frame from Responses
raw_queries_df = (spark.createDataFrame(all_responses))
raw_queries_df.createOrReplaceTempView("raw")

# COMMAND ----------

# DBTITLE 1,Create Database to Store Results
spark.sql("""CREATE DATABASE IF NOT EXISTS delta_optimizer;""")

# COMMAND ----------

# DBTITLE 1,Parse Reponse for Columns we need to calculate statistics
spark.sql("""CREATE OR REPLACE TABLE delta_optimizer.raw_query_history_statistics
      AS
    SELECT
    query_id,
    query_start_time_ms,
    query_end_time_ms,
    duration,
    query_text,
    status,
    statement_type,
    rows_produced,
    metrics
    FROM raw
    WHERE statement_type = 'SELECT';
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM delta_optimizer.raw_query_history_statistics

# COMMAND ----------

# DBTITLE 1,Calculate Summary Statistics
spark.sql("""
--Calculate Query Statistics to get importance Rank by Query (duration, rows_returned)

CREATE OR REPLACE TABLE delta_optimizer.query_summary_statistics
AS (
  WITH raw_query_stats AS (
    SELECT query_id,
    AVG(duration) AS AverageQueryDuration,
    AVG(rows_produced) AS AverageRowsProduced,
    COUNT(*) AS TotalQueryRuns,
    AVG(duration)*COUNT(*) AS DurationTimesRuns
    FROM delta_optimizer.raw_query_history_statistics
    WHERE status IN('FINISHED', 'CANCELED')
    AND statement_type = 'SELECT'
    GROUP BY query_id
  )
  SELECT 
  *
  FROM raw_query_stats
)

""")

# COMMAND ----------

# DBTITLE 1,Parse SQL Tree for filtered columns and table map
## Input Filter Type can be : where, join, group_by

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
      final_columns.append(results.columns_dict.get("group_by"))

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

# DBTITLE 1,Helper functions to differentiate how columns are used (important for stats and Zorder)
@udf("integer")
def checkIfJoinColumn(sqlString, columnName):
    try: 
        results = Parser(sqlString)

        ## If just a select, then skip this and just return the table
        if columnName in results.columns_dict.get("join"):
            return 1
        else:
            return 0

    except:
        return 0
    
    
    
@udf("integer")
def checkIfFilterColumn(sqlString, columnName):
    try: 
        results = Parser(sqlString)

        ## If just a select, then skip this and just return the table
        if columnName in results.columns_dict.get("where"):
            return 1
        else:
            return 0

    except:
        return 0
    
    
@udf("integer")
def checkIfGroupColumn(sqlString, columnName):
    try: 
        results = Parser(sqlString)

        ## If just a select, then skip this and just return the table
        if columnName in results.columns_dict.get("group_by"):
            return 1
        else:
            return 0

    except:
        return 0

# COMMAND ----------

spark.sql("""DROP TABLE IF EXISTS delta_optimizer.parsed_distinct_queries""")

# COMMAND ----------

# DBTITLE 1,Use Udf To Parse Query Text
df_profiled_where = (df.withColumn("profiled_columns", getParsedFilteredColumnsinSQL(F.col("query_text")))
                    )
#df_profiled.createOrReplaceTempView("parsed")

df_profiled_where.write.format("delta").saveAsTable("delta_optimizer.parsed_distinct_queries")

# COMMAND ----------

# DBTITLE 1,Calculate Statistics

# OUTPUT: table with table_name, column_name, # query filter references, # total execution referencing, # avg duration of query when referenced

pre_stats_df = (spark.sql("""
  WITH exploded_parsed_cols AS (SELECT DISTINCT
  explode(profiled_columns) AS explodedCols,
  query_id,
  query_text
  FROM delta_optimizer.parsed_distinct_queries
  ),

  step_2 AS (SELECT DISTINCT
  split(explodedCols, ":")[0] AS TableName,
  split(explodedCols, ":")[1] AS ColumnName,
  root.query_text,
  hist.*
  FROM exploded_parsed_cols AS root
  LEFT JOIN delta_optimizer.query_summary_statistics AS hist USING (query_id)--SELECT statements only included
  )
  
  SELECT *,
  size(split(query_text, ColumnName)) - 1 AS NumberOfColumnOccurrences
  FROM step_2
""")
.withColumn("isUsedInJoin", checkIfJoinColumn(F.col("query_text"), F.concat(F.col("TableName"), F.lit("."), F.col("ColumnName"))))
.withColumn("isUsedInFilter", checkIfFilterColumn(F.col("query_text"), F.concat(F.col("TableName"), F.lit("."), F.col("ColumnName"))))
.withColumn("isUsedInGroup", checkIfGroupColumn(F.col("query_text"), F.concat(F.col("TableName"), F.lit("."), F.col("ColumnName"))))
)
               
pre_stats_df.createOrReplaceTempView("withUseFlags")

spark.sql("""
CREATE OR REPLACE TABLE delta_optimizer.query_column_statistics
AS (
SELECT * FROM withUseFlags
)
""")

# COMMAND ----------

spark.sql("""CREATE OR REPLACE TABLE delta_optimizer.read_statistics_column_level_summary
AS
WITH test_q AS (
SELECT * FROM delta_optimizer.query_column_statistics
WHERE length(ColumnName) >= 1 -- filter out queries with no joins or predicates
AND CASE WHEN "${Database}" != "All" THEN TableName LIKE (CONCAT("${Database}" ,"%")) ELSE true END
),
step_2 AS (
SELECT 
TableName,
ColumnName,
MAX(isUsedInJoin) AS isUsedInJoin,
MAX(isUsedInFilter) AS isUsedInFilter,
MAX(isUsedInGroup) AS isUsedInGroup,
SUM(isUsedInJoin) AS NumberOfQueriesUsedInJoin,
SUM(isUsedInFilter) AS NumberOfQueriesUsedInFilter,
SUM(isUsedInGroup) AS NumberOfQueriesUsedInGroup,
COUNT(DISTINCT query_id) AS QueryReferenceCount,
SUM(DurationTimesRuns) AS RawTotalRuntime,
AVG(AverageQueryDuration) AS AvgQueryDuration,
SUM(NumberOfColumnOccurrences) AS TotalColumnOccurrencesForAllQueries,
AVG(NumberOfColumnOccurrences) AS AvgColumnOccurrencesInQueryies
FROM test_q
WHERE length(ColumnName) >=1
GROUP BY TableName, ColumnName
)
SELECT 
*
FROM step_2
; """)

# COMMAND ----------

# DBTITLE 1,Scale Query Statistics

df = spark.sql("""SELECT * FROM delta_optimizer.read_statistics_column_level_summary""")

columns_to_scale = ["QueryReferenceCount", "RawTotalRuntime", "AvgQueryDuration", "TotalColumnOccurrencesForAllQueries", "AvgColumnOccurrencesInQueryies"]
min_exprs = {x: "min" for x in columns_to_scale}
max_exprs = {x: "max" for x in columns_to_scale}

## Apply basic min max scaling by table for now

dfmin = df.groupBy("TableName").agg(min_exprs)
dfmax = df.groupBy("TableName").agg(max_exprs)

df_boundaries = dfmin.join(dfmax, on="TableName", how="inner")

df_pre_scaled = df.join(df_boundaries, on="TableName", how="inner")

df_scaled = (df_pre_scaled
         .withColumn("QueryReferenceCountScaled", coalesce((col("QueryReferenceCount") - col("min(QueryReferenceCount)"))/(col("max(QueryReferenceCount)") - col("min(QueryReferenceCount)")), lit(0)))
         .withColumn("RawTotalRuntimeScaled", coalesce((col("RawTotalRuntime") - col("min(RawTotalRuntime)"))/(col("max(RawTotalRuntime)") - col("min(RawTotalRuntime)")), lit(0)))
         .withColumn("AvgQueryDurationScaled", coalesce((col("AvgQueryDuration") - col("min(AvgQueryDuration)"))/(col("max(AvgQueryDuration)") - col("min(AvgQueryDuration)")), lit(0)))
         .withColumn("TotalColumnOccurrencesForAllQueriesScaled", coalesce((col("TotalColumnOccurrencesForAllQueries") - col("min(TotalColumnOccurrencesForAllQueries)"))/(col("max(TotalColumnOccurrencesForAllQueries)") - col("min(TotalColumnOccurrencesForAllQueries)")), lit(0)))
         .withColumn("AvgColumnOccurrencesInQueriesScaled", coalesce((col("AvgColumnOccurrencesInQueryies") - col("min(AvgColumnOccurrencesInQueryies)"))/(col("max(AvgColumnOccurrencesInQueryies)") - col("min(AvgColumnOccurrencesInQueryies)")), lit(0)))
         .selectExpr("TableName", "ColumnName", "isUsedInJoin", "isUsedInFilter","isUsedInGroup","NumberOfQueriesUsedInJoin","NumberOfQueriesUsedInFilter","NumberOfQueriesUsedInGroup","QueryReferenceCount", "RawTotalRuntime", "AvgQueryDuration", "TotalColumnOccurrencesForAllQueries", "AvgColumnOccurrencesInQueryies", "QueryReferenceCountScaled", "RawTotalRuntimeScaled", "AvgQueryDurationScaled", "TotalColumnOccurrencesForAllQueriesScaled", "AvgColumnOccurrencesInQueriesScaled")
            )


# COMMAND ----------

df_scaled.createOrReplaceTempView("final_scaled_reads")

spark.sql("""CREATE OR REPLACE TABLE delta_optimizer.read_statistics_scaled_results AS SELECT * FROM final_scaled_reads""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM delta_optimizer.read_statistics_scaled_results
