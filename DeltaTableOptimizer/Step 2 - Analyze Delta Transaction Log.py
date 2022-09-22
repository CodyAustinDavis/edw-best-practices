# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### For all tables in a given database/catalog level, get the transaction log and find the columns most used in the MERGE or other predicates and collect stats
# MAGIC 
# MAGIC ### RETURNS
# MAGIC 
# MAGIC 1. delta_optimizer.write_statistics_merge_predicate -- Has stats on merge predicate usage by table and column
# MAGIC 2. delta_optimizer.all_tables_cardinality_stats -- Has stats on cardinality propotion from a sample of each column/table
# MAGIC 3. delta_optimizer.all_tables_table_stats -- Has stats on table size and partitions, with map with target file size in Delta
# MAGIC 
# MAGIC <b> Dependencies: </b> None
# MAGIC 
# MAGIC 
# MAGIC <b> Roadmap: </b> 
# MAGIC <li> 1. Get Size of Table to set file size -- DONE
# MAGIC <li> 2. Get Size of table to decide how often and which columns to collect stats on

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

# DBTITLE 1,Static File Size Mapping
file_size_map = [{"max_table_size_gb": 8, "file_size": '16mb'},
                 {"max_table_size_gb": 16, "file_size": '32mb'},
                 {"max_table_size_gb": 32, "file_size": '64mb'},
                 {"max_table_size_gb": 64, "file_size": '128mb'},
                 {"max_table_size_gb": 128, "file_size": '128mb'},
                 {"max_table_size_gb": 256, "file_size": '256mb'},
                 {"max_table_size_gb": 512, "file_size": '256mb'},
                 {"max_table_size_gb": 1024, "file_size": '307mb'},
                 {"max_table_size_gb": 2560, "file_size": '512mb'},
                 {"max_table_size_gb": 3072, "file_size": '716mb'},
                 {"max_table_size_gb": 5120, "file_size": '1gb'},
                 {"max_table_size_gb": 7168, "file_size": '1gb'},
                 {"max_table_size_gb": 10240, "file_size": '1gb'},
                 {"max_table_size_gb": 51200, "file_size": '1gb'},
                 {"max_table_size_gb": 102400, "file_size": '1gb'}]


#file_size_map = json.dump(file_size_map)
file_size_df = (spark.createDataFrame(file_size_map))


display(file_size_df)

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

# DBTITLE 1,Create Target Statistics Tables
spark.sql("""CREATE TABLE IF NOT EXISTS delta_optimizer.all_tables_cardinality_stats
             (TableName STRING,
             ColumnName STRING,
             SampleSize INTEGER,
             TotalCountInSample INTEGER,
             DistinctCountOfColumnInSample INTEGER,
             CardinalityProportion FLOAT,
             CardinalityProportionScaled FLOAT,
             IsUsedInReads INTEGER,
             IsUsedInWrites INTEGER)
             USING DELTA""")



spark.sql("""CREATE TABLE IF NOT EXISTS delta_optimizer.all_tables_table_stats
             (TableName STRING,
             sizeInBytes FLOAT,
             sizeInGB FLOAT,
             partitionColumns ARRAY<STRING>,
             mappedFileSizeInMb STRING)
             USING DELTA""")

# COMMAND ----------

# DBTITLE 1,Build Table Cardinality and Sizing Stats
for tbl in table_list:
    
    print(f"Prepping Delta Table Stats: {tbl}")
    
    try: 
        df_cols = [Row(i) for i in spark.sql(f"""SELECT * FROM {tbl}""").columns]

        df = sc.parallelize(df_cols).toDF().withColumnRenamed("_1", "ColumnName").withColumn("TableName", lit(tbl))

        df.createOrReplaceTempView("df_cols")
        
        spark.sql("""INSERT INTO delta_optimizer.all_tables_cardinality_stats
        SELECT TableName, ColumnName, NULL, NULL, NULL, NULL, NULL, NULL, NULL FROM df_cols AS source
        WHERE NOT EXISTS (SELECT 1 FROM delta_optimizer.all_tables_cardinality_stats ss WHERE ss.TableName = source.TableName AND ss.ColumnName = source.ColumnName)
        """)
        
    except Exception as e:
        print(f"Skipping analysis for table {tbl} for error: {str(e)}")
        pass
      
    print(f"Collecting Size and Partition Stats for : {tbl}")
    
    
    try: 
        table_df = (spark.sql(f"""DESCRIBE DETAIL {tbl}""")
            .selectExpr("name", "sizeInBytes", "sizeInBytes/(1024*1024*1024) AS sizeInGB", "partitionColumns")
                    )
        
        table_df.createOrReplaceTempView("table_core")
        file_size_df.createOrReplaceTempView("file_size_map")
        
        spark.sql("""
        WITH ss AS (
            SELECT 
            spine.*,
            file_size AS mapped_file_size,
            ROW_NUMBER() OVER (PARTITION BY name ORDER BY max_table_size_gb) AS SizeRank
            FROM table_core AS spine
            LEFT JOIN file_size_map AS fs ON spine.sizeInGB::integer <= fs.max_table_size_gb::integer
            )
            -- Pick smaller file size config by table size
            INSERT INTO delta_optimizer.all_tables_table_stats
            SELECT
            name::string AS TableName, 
            sizeInBytes::float AS sizeInBytes,
            sizeInGB::float AS sizeInGB,
            partitionColumns::array<string> AS partitionColumns,
            mapped_file_size::string AS mappedFileSize
            FROM ss WHERE SizeRank = 1 
          """)
    except Exception as e:
        
        print(f"Failed to parse stats for {tbl} with error: {str(e)}")
        
        continue

# COMMAND ----------

spark.sql("""OPTIMIZE delta_optimizer.all_tables_cardinality_stats""")
spark.sql("""OPTIMIZE delta_optimizer.all_tables_table_stats""")
