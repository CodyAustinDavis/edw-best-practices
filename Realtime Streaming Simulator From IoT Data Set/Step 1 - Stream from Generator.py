# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # This notebook generates a full data pipeline from databricks dataset - iot-stream using Autoloader + Structured Streaming
# MAGIC 
# MAGIC ## This creates 2 tables: 
# MAGIC 
# MAGIC <b> Database: </b> real_time_iot_dashboard
# MAGIC 
# MAGIC <b> Tables: </b> silver_sensors, silver_users 
# MAGIC 
# MAGIC <b> Params: </b> StartOver (Yes/No) - allows user to truncate and reload pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2022/03/delta-lake-medallion-architecture-2.jpeg" >

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://miro.medium.com/max/1400/1*N2hJnle6RJ6HRRF4ISFBjw.gif">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Autoloader Benefits: 
# MAGIC 
# MAGIC 1. More Scalable Directory Listing (incremental + file notification)
# MAGIC 2. Rocks DB State Store - Faster State management
# MAGIC 3. Schema Inference + Merge Schema: https://docs.databricks.com/ingestion/auto-loader/schema.html
# MAGIC 4. File Notification Mode - For ultra high file volumes from S3/ADLS/GCS: https://docs.databricks.com/ingestion/auto-loader/file-detection-modes.html
# MAGIC 5. Complex Sources -- advanced Glob Path Filters: <b> .option("pathGlobFilter", "[a-zA-Z].csv") </b> 
# MAGIC 6. Rescue data - automatically insert "bad" data into a rescued data column so you never lose data <b> .option("cloudFiles.rescuedDataColumn", "_rescued_data")  </b>
# MAGIC 7: Flexible Schema Hints: <b> .option("cloudFiles.schemaHints", "tags map<string,string>, version int") </b> 
# MAGIC 
# MAGIC Much more!
# MAGIC 
# MAGIC ### Auto loader intro: 
# MAGIC https://docs.databricks.com/ingestion/auto-loader/index.html
# MAGIC 
# MAGIC Rescue Data: https://docs.databricks.com/ingestion/auto-loader/schema.html#rescue
# MAGIC 
# MAGIC 
# MAGIC ### Auto Loader Full Options: 
# MAGIC 
# MAGIC https://docs.databricks.com/ingestion/auto-loader/options.html
# MAGIC   
# MAGIC ## Meta data options: 
# MAGIC 
# MAGIC Load the file metadata in auto loader for downstream continuity
# MAGIC   https://docs.databricks.com/ingestion/file-metadata-column.html
# MAGIC   

# COMMAND ----------

dbutils.widgets.dropdown("StartOver", "Yes", ["No", "Yes"])

start_over = dbutils.widgets.get("StartOver")

print(f"Start Over?: {start_over}")

# COMMAND ----------

# DBTITLE 1,Do not edit - Standard file paths
## Output path of real-time data generator
file_source_location = "dbfs:/Filestore/real-time-data-demo/iot_dashboard/"
checkpoint_location = f"dbfs:/FileStore/real-time-data-demo/checkpoints/IotDemoCheckpoints/AutoloaderDemo/bronze"
checkpoint_location_silver = f"dbfs:/FileStore/real-time-data-demo/checkpoints/silver"
autoloader_schema_location = f"dbfs:/FileStore/real-time-data-demo/schema_checkpoints/AutoloaderDemoSchema/"
database_name = "real_time_iot_dashboard"

# COMMAND ----------

# DBTITLE 1,Look at Raw Data Source
dbutils.fs.ls(file_source_location)

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid

# COMMAND ----------

#### Register udf for generating UUIDs
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

# DBTITLE 1,Create Database
spark.sql(f"""CREATE DATABASE IF NOT EXISTS {database_name};""")
##LOCATION 's3a://<path>' or 'adls://<path>'

# COMMAND ----------

# DBTITLE 1,Start Over
if start_over == "Yes":
  
  spark.sql(f"""DROP TABLE IF EXISTS {database_name}.bronze_sensors""")
  spark.sql(f"""DROP TABLE IF EXISTS {database_name}.silver_sensors""")
  spark.sql(f"""DROP TABLE IF EXISTS {database_name}.bronze_users""")
  spark.sql(f"""DROP TABLE IF EXISTS {database_name}.silver_users""")

# COMMAND ----------

# DBTITLE 1,Let Autoloader Sample files for schema inference - backfill interval can refresh this inference
spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", 10) ## 1000 is default

# COMMAND ----------

# DBTITLE 1,Re-infer schema if starting over
if start_over == "Yes":
  dbutils.fs.rm(autoloader_schema_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,Read Stream with Autoloader - LOTS of Options for any type of data
df_raw = (spark
     .readStream
     .format("cloudFiles") ## csv, json, binary, text, parquet, avro
     .option("cloudFiles.format", "text")
     .option("cloudFiles.useIncrementalListing", "true")
     #.option("cloudFiles.useNotifications", "true")
     .option("cloudFiles.schemaLocation", autoloader_schema_location)
     #.option("schema", inputSchema)
     #.option("modifiedAfter", timestampString) ## option
     .option("cloudFiles.maxFilesPerTrigger", 100000) ## maxBytesPerTrigger, 10mb
     #.option("pathGlobFilter", "*.json*") ## Only certain files ## regex expr
     .option("ignoreChanges", "true")
     #.option("ignoreDeletes", "true")
     .option("latestFirst", "false")
     .load(file_source_location)
     .selectExpr(
                "value:device_id::integer AS device_id",
                "value:user_id::integer AS user_id",
                "value:calories_burnt::decimal AS calories_burnt",
                "value:miles_walked::decimal AS miles_walked",
                "value:num_steps::decimal AS num_steps",
                "value:time_stamp::timestamp AS timestamp",
                "value::string AS raw_data"
     )
     .withColumn("id", uuidUdf())
     #.select("*", "_metadata") ##_metadata exits with DBR 11.0 + 
     .withColumn("InputFileName", input_file_name())
    )

# COMMAND ----------

# DBTITLE 1,Display a Stream for testing
display(df_raw)

# COMMAND ----------

# DBTITLE 1,Delete Checkpoint Location to start over
if start_over == "Yes":
  dbutils.fs.rm(checkpoint_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,Write Stream -- same as any other stream
## with DBR 11.2+, Order insertion clustering is automatic!

(df_raw
 .withColumn("date_month", date_trunc('month', col("timestamp")))
.writeStream
.format("delta")
.option("checkpointLocation", checkpoint_location)
.trigger(processingTime = '2 seconds') ## once=True, processingTime = '5 minutes', continuous ='1 minute'
#.partitionBy("date_month") ## DO NOT OVER PARTITION
.toTable(f"{database_name}.bronze_sensors") ## We do not need to define the DDL, it will be created on write, but we can define DDL if we want to
)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Now we have data streaming into a bronze table at any clip/rate we want. 
# MAGIC #### How can we stream into a silver table with merge?

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <b> Stream options from a delta table: </b> https://docs.databricks.com/delta/delta-streaming.html
# MAGIC 
# MAGIC 
# MAGIC <li> <b> 1. Limit throughput rate: </b>  https://docs.databricks.com/delta/delta-streaming.html#limit-input-rate
# MAGIC <li> <b> 2. Specify starting version or timestamp: </b>  https://docs.databricks.com/delta/delta-streaming.html#specify-initial-position
# MAGIC <li> <b> 3. Ignore updates/deletes: </b>  https://docs.databricks.com/delta/delta-streaming.html#ignore-updates-and-deletes

# COMMAND ----------


## You can use this to see how often to trigger visuals directly in Dash!
history_df = spark.sql(f"""DESCRIBE HISTORY {database_name}.bronze_sensors;""")

display(history_df)

# COMMAND ----------

# DBTITLE 1,Pro Tip: Use Transaction Log Metadata to build intelligent Data Apps
# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC WITH log AS
# MAGIC (DESCRIBE HISTORY real_time_iot_dashboard.bronze_sensors
# MAGIC ),
# MAGIC state AS (
# MAGIC SELECT
# MAGIC version,
# MAGIC timestamp,
# MAGIC operation
# MAGIC FROM log
# MAGIC WHERE (timestamp >= current_timestamp() - INTERVAL '24 hours')
# MAGIC AND operation IN ('MERGE', 'WRITE', 'DELETE', 'STREAMING UPDATE')
# MAGIC ORDER By version DESC
# MAGIC ),
# MAGIC comparison AS (
# MAGIC SELECT DISTINCT
# MAGIC s1.version,
# MAGIC s1.timestamp,
# MAGIC s1.operation,
# MAGIC LAG(version) OVER (ORDER BY version) AS Previous_Version,
# MAGIC LAG(timestamp) OVER (ORDER BY timestamp) AS Previous_Timestamp
# MAGIC FROM state AS s1
# MAGIC ORDER BY version DESC)
# MAGIC 
# MAGIC SELECT
# MAGIC date_trunc('hour', timestamp) AS HourBlock,
# MAGIC AVG(timestamp::double - Previous_Timestamp::double) AS AvgUpdateFrequencyInSeconds
# MAGIC FROM comparison
# MAGIC GROUP BY date_trunc('hour', timestamp)
# MAGIC ORDER BY HourBlock

# COMMAND ----------

# DBTITLE 1,Stream with Delta options
df_bronze = (
  spark.readStream
#.option("startingVersion", "1") ## Or .option("startingTimestamp", "2018-10-18") You can optionally explicitly state which version to start streaming from
.option("ignoreChanges", "true") ## .option("ignoreDeletes", "true")
#.option("useChangeFeed", "true")
.option("maxFilesPerTrigger", 100000) ## Optional - FIFO processing
.table(f"{database_name}.bronze_sensors")
)

#display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## If your data has updates and is not sub-second latency, you can use Structured Streaming to MERGE anything at any clip!

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <b> WARNING: DO NOT USE MERGE FOR REAL-TIME PIPELINES - use watermarking instead </b>

# COMMAND ----------

# DBTITLE 1,Create Target Silver Table for Merge
spark.sql(f"""CREATE OR REPLACE TABLE {database_name}.silver_sensors
AS SELECT * FROM {database_name}.bronze_sensors WHERE 1=2;""")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Use Structured Streaming for ANY EDW Workload with this design Pattern!

# COMMAND ----------

# DBTITLE 1,Define function to run on each microBatch (every 1s - every day, anything)
from delta.tables import *
from pyspark.sql import Row, functions as F
from pyspark.sql.window import Window

def mergeStatementForMicroBatch(microBatchDf, microBatchId):
  
  """
  #### Python way
  silverDeltaTable = DeltaTable.forName(spark, f"{database_name}.silver_sensors")
  
  ## Delete duplicates in source if there are any 
  
  updatesDeDupedInMicroBatch = (microBatchDf
                                .select("*", 
                                   F.row_number().over(Window.partitionBy("Id", "user_id", "device_id", "timestamp").orderBy("timestamp")).alias("dupRank")
                                       )
                                .filter(col("dupRank") == lit("1")) ## Get only 1 most recent copy per row
                               ) ## partition on natural key or pk for dups within a microBatch if there are any
  
  (silverDeltaTable.alias("target")
  .merge(
    updatesDeDupedInMicroBatch.distinct().alias("updates"),
    "target.Id = updates.Id AND updates.user_id = target.user_id AND target.device_id = updates.device_id"
        )
   .whenMatchedUpdate(set =
    {
      "calories_burnt": "updates.calories_burnt",
      "miles_walked": "updates.miles_walked",
      "num_steps": "updates.num_steps",
      "timestamp": "updates.timestamp"
    }
  )
  .whenNotMatchedInsertAll()
  .execute()
  )
  """
  ### SQL Way to do it inside the micro batch
  
  ## Register microbatch in SQL temp table and run merge using spark.sql
  microBatchDf.createOrReplaceGlobalTempView("updates_df")
  
  spark.sql(f"""
  
  MERGE INTO {database_name}.silver_sensors AS target
  USING (
         SELECT *
         FROM (
           SELECT *,ROW_NUMBER() OVER(PARTITION BY id, user_id, device_id, timestamp ORDER BY timestamp DESC) AS DupRank
           FROM global_temp.updates_df
             )
         WHERE DupRank = 1
         )
                AS source
  ON source.id = target.id
  AND source.user_id = target.user_id
  AND source.device_id = target.device_id
  WHEN MATCHED THEN UPDATE SET 
    target.calories_burnt = source.calories_burnt,
    target.miles_walked = source.miles_walked,
    target.num_steps = source.num_steps,
    target.timestamp = source.timestamp
  WHEN NOT MATCHED THEN INSERT *;
  """)
  
  ## optimize table after the merge for faster queries
  spark.sql(f"""OPTIMIZE {database_name}.silver_sensors ZORDER BY (timestamp)""")
  
  return

# COMMAND ----------

# DBTITLE 1,Delete checkpoint - each stream has 1 checkpoint
dbutils.fs.rm(checkpoint_location_silver, recurse=True)


# COMMAND ----------

# DBTITLE 1,Write Stream as often as you want
(df_bronze
.writeStream
.option("checkpointLocation", checkpoint_location_silver)
.trigger(processingTime='3 seconds') ## processingTime='1 minute' -- Now we can run this merge every minute!
.foreachBatch(mergeStatementForMicroBatch)
.start()
)

# COMMAND ----------

silver_df = spark.sql(f"""SELECT * FROM {database_name}.silver_sensors;""")

display(silver_df)
