# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### For all tables in a given database/catalog level, get the transaction log and find the columns most used in the MERGE or other predicates and collect stats
# MAGIC 
# MAGIC ### RETURNS
# MAGIC 
# MAGIC 1. 
# MAGIC 
# MAGIC <b> Dependencies: </b> None

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row


# COMMAND ----------

dbutils.widgets.text("Database Names (csv):", "")
dbutils.widgets.dropdown("Mode (all databases or a subset)", "Subset", ["All", "Subset"])
database = dbutils.widgets.get("Database Names (csv):")
db_mode = dbutils.widgets.get("Mode (all databases or a subset)")

# COMMAND ----------

db_list = [x.strip() for x in database.split(",") if x != '']


tbl_df = spark.sql("show tables in default like 'xxx'")
#Loop through all databases
for db in spark.sql("show databases").filter(col("databaseName").isin(db_list)).collect():
  #create a dataframe with list of tables from the database
  df = spark.sql(f"show tables in {db.databaseName}")
  #union the tables list dataframe with main dataframe 
  tbl_df = tbl_df.union(df)
  
if db_mode == "All" or len(db_list) == 0:
  
  print("Getting transaction logs for all tables and databases")
  
  df = (tbl_df
      .filter(col("isTemporary") == lit('false'))
     )
  
elif db_mode == "Subset":
  
  print(f"Getting transaction logs for all tables in {db_list}")
  
  df = (tbl_df
        .filter(col("database").isin(db_list))
      .filter(col("isTemporary") == lit('false'))
     )

df.createOrReplaceTempView("all_tables")


# COMMAND ----------

df_tables = spark.sql("""
SELECT 
concat(database, '.', tableName) AS fully_qualified_table_name
FROM all_tables
""").collect()


table_list = [i[0] for i in df_tables]

print(f"Running Merge Predicate Analysis for: \n {table_list}")

# COMMAND ----------

spark.sql("""CREATE TABLE IF NOT EXISTS delta_optimizer.merge_predicate_statistics
(
TableName STRING,
TableColumns STRING,
HasColumnInMergePredicate INTEGER,
NumberOfVersionsPredicateIsUsed INTEGER,
AvgMergeRuntimeMs INTEGER,
UpdateTimestamp TIMESTAMP)
USING DELTA;
""")

# COMMAND ----------

# DBTITLE 1,Load Predicate Stats for All Tables in Selected Database
for tbl in  table_list: 
  
  ## Get Transaction log with relevant transactions
  hist_df = spark.sql(f"""
  WITH hist AS
  (DESCRIBE HISTORY {tbl}
  )

  SELECT version, timestamp,
  operationParameters.predicate,
  operationMetrics.executionTimeMs
  FROM hist
  WHERE operation = 'MERGE'
  ;
  """)

  hist_df.createOrReplaceTempView("hist_df")
  
  ## Get DF of Columns for that table
  
  df_cols = [Row(i) for i in spark.sql(f"""SELECT * FROM {tbl}""").columns]

  df = sc.parallelize(df_cols).toDF().withColumnRenamed("_1", "Columns")

  df.createOrReplaceTempView("df_cols")
  
  ## Calculate stats for this table
  
  df_stats = (spark.sql("""
    -- Full Cartesian product small table.. maybe since one table at a time... parallelize later 
    WITH raw_results AS (
    SELECT 
    *,
    predicate LIKE (concat('%',`Columns`::string,'%')) AS HasColumnInMergePredicate
    FROM df_cols
    JOIN hist_df
    )

    SELECT Columns AS TableColumns,
    CASE WHEN MAX(HasColumnInMergePredicate) = 'true' THEN 1 ELSE 0 END AS HasColumnInMergePredicate,
    COUNT(DISTINCT CASE WHEN HasColumnInMergePredicate = 'true' THEN `version` ELSE NULL END)::integer AS NumberOfVersionsPredicateIsUsed,
    AVG(executionTimeMs::integer) AS AvgMergeRuntimeMs
    FROM raw_results
    GROUP BY Columns
    """)
                .withColumn("TableName", lit(tbl))
                .withColumn("UpdateTimestamp", current_timestamp())
                .select("TableName", "TableColumns", "HasColumnInMergePredicate", "NumberOfVersionsPredicateIsUsed", "AvgMergeRuntimeMs", "UpdateTimestamp")
               )
  
  (df_stats.createOrReplaceTempView("source_stats"))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO delta_optimizer.merge_predicate_statistics AS target
# MAGIC USING source_stats AS source
# MAGIC ON source.TableName = target.TableName AND source.TableColumns = target.TableColumns
# MAGIC WHEN MATCHED THEN UPDATE SET * 
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM delta_optimizer.merge_predicate_statistics ORDER BY UpdateTimestamp DESC
