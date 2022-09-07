# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### For all tables in a given database/catalog level, get the transaction log and find the columns most used in the MERGE or other predicates and collect stats
# MAGIC 
# MAGIC ### RETURNS
# MAGIC 
# MAGIC 1. delta_optimizer.write_statistics_merge_predicate
# MAGIC 
# MAGIC <b> Dependencies: </b> None
# MAGIC 
# MAGIC 
# MAGIC <b> Roadmap: </b> 
# MAGIC <li> 1. Get Size of Table to set file size 
# MAGIC <li> 2. Get Size of table to decide how often and which columns to collect stats on
# MAGIC <li> 3. If there is a MERGE predicate, create DDL to tune file sizes for re-writes automatically

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

# DBTITLE 1,List All Table in the Selected Databases to Analyze
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

# DBTITLE 1,Get Final List of Tables
df_tables = spark.sql("""
SELECT 
concat(database, '.', tableName) AS fully_qualified_table_name
FROM all_tables
""").collect()


table_list = [i[0] for i in df_tables]

print(f"Running Merge Predicate Analysis for: \n {table_list}")

# COMMAND ----------

spark.sql("""CREATE DATABASE IF NOT EXISTS delta_optimizer""")

# COMMAND ----------

spark.sql("""CREATE OR REPLACE TABLE delta_optimizer.write_statistics_merge_predicate
            (
            TableName STRING,
            ColumnName STRING,
            HasColumnInMergePredicate INTEGER,
            NumberOfVersionsPredicateIsUsed INTEGER,
            AvgMergeRuntimeMs INTEGER,
            UpdateTimestamp TIMESTAMP)
            USING DELTA;
""")

# COMMAND ----------

# DBTITLE 1,Load Predicate Stats for All Tables in Selected Database
for tbl in table_list:
    print(f"Running History Analysis for Table: {tbl}")
    
    try: 
        
        ## Get Transaction log with relevant transactions
        hist_df = spark.sql(f"""
        WITH hist AS
        (DESCRIBE HISTORY {tbl}
        )

        SELECT version, timestamp,
        operationParameters.predicate,
        operationMetrics.executionTimeMs
        FROM hist
        WHERE operation IN ('MERGE', 'DELETE')
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

        SELECT Columns AS ColumnName,
        CASE WHEN MAX(HasColumnInMergePredicate) = 'true' THEN 1 ELSE 0 END AS HasColumnInMergePredicate,
        COUNT(DISTINCT CASE WHEN HasColumnInMergePredicate = 'true' THEN `version` ELSE NULL END)::integer AS NumberOfVersionsPredicateIsUsed,
        AVG(executionTimeMs::integer) AS AvgMergeRuntimeMs
        FROM raw_results
        GROUP BY Columns
        """)
                    .withColumn("TableName", lit(tbl))
                    .withColumn("UpdateTimestamp", current_timestamp())
                    .select("TableName", "ColumnName", "HasColumnInMergePredicate", "NumberOfVersionsPredicateIsUsed", "AvgMergeRuntimeMs", "UpdateTimestamp")
                   )

        (df_stats.createOrReplaceTempView("source_stats"))

        spark.sql("""MERGE INTO delta_optimizer.write_statistics_merge_predicate AS target
        USING source_stats AS source
        ON source.TableName = target.TableName AND source.ColumnName = target.ColumnName
        WHEN MATCHED THEN UPDATE SET * 
        WHEN NOT MATCHED THEN INSERT *
        """)
        
    except Exception as e:
        print(f"Skipping analysis for table {tbl} for error: {str(e)}")
        pass

# COMMAND ----------

spark.sql("""CREATE TABLE IF NOT EXISTS delta_optimizer.all_tables_cardinality_stats
             (TableName STRING,
             ColumnName STRING,
             SampleSize INTEGER,
             TotalCountInSample INTEGER,
             DistinctCountOfColumnInSample INTEGER,
             CardinalityProportion FLOAT,
             IsUsedInReads INTEGER,
             IsUsedInWrites INTEGER)
             USING DELTA""")

# COMMAND ----------

for tbl in table_list:
    
    print(f"Prepping Delta Table Stats: {tbl}")
    
    try: 
        df_cols = [Row(i) for i in spark.sql(f"""SELECT * FROM {tbl}""").columns]

        df = sc.parallelize(df_cols).toDF().withColumnRenamed("_1", "ColumnName").withColumn("TableName", lit(tbl))

        df.createOrReplaceTempView("df_cols")
        
        spark.sql("""INSERT INTO delta_optimizer.all_tables_cardinality_stats
        SELECT TableName, ColumnName, NULL, NULL, NULL, NULL, NULL, NULL FROM df_cols AS source
        WHERE NOT EXISTS (SELECT 1 FROM delta_optimizer.all_tables_cardinality_stats ss WHERE ss.TableName = source.TableName AND ss.ColumnName = source.ColumnName)
        """)
        
    except Exception as e:
        print(f"Skipping analysis for table {tbl} for error: {str(e)}")
        pass

# COMMAND ----------

spark.sql("""OPTIMIZE delta_optimizer.all_tables_cardinality_stats""")
