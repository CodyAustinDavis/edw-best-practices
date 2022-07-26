# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### For all tables in a given database/catalog level, get the transaction log and find the columns most used in the MERGE or other predicates and collect stats
# MAGIC 
# MAGIC <b> Dependencies: </b> None

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row


# COMMAND ----------

dbutils.widgets.text("Database Name: ", "")

database = dbutils.widgets.get("Database Name: ")

# COMMAND ----------

df = (spark.sql(f"""SHOW TABLES IN {database}""")
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
  operationParameters.predicate
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
    COUNT(DISTINCT CASE WHEN HasColumnInMergePredicate = 'true' THEN `version` ELSE NULL END)::integer AS NumberOfVersionsPredicateIsUsed
    FROM raw_results
    GROUP BY Columns
    """)
                .withColumn("TableName", lit(tbl))
                .withColumn("UpdateTimestamp", current_timestamp())
                .select("TableName", "TableColumns", "HasColumnInMergePredicate", "NumberOfVersionsPredicateIsUsed", "UpdateTimestamp")
               )
  
  (df_stats.write.format("delta").mode("append").saveAsTable("delta_optimizer.merge_predicate_statistics"))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM delta_optimizer.merge_predicate_statistics ORDER BY UpdateTimestamp DESC
