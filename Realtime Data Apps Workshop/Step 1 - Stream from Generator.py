# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Building Effective Streaming Pipelines -- Autoloader, Delta, Merge
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Overview**: This notebook will provide a broad overview on some key practices for streaming data from files using autoloader, streaming to and from delta, and unifying batch and streaming MERGE workloads
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 3 Parts: 
# MAGIC 
# MAGIC <li> 1. Raw to Bronze Streaming with Autoloader
# MAGIC   
# MAGIC <li> 2. Bronze (or even raw) to Silver Streaming with Watermarking
# MAGIC   
# MAGIC <li> 3. Bronze to Silver Streaming with foreachBatch AND MERGE (for less real-time, more EDW like workloads)
# MAGIC   
# MAGIC 
# MAGIC ---
# MAGIC   
# MAGIC ### This notebook creates 2 tables from the sample data set in the asssociated data generator: 
# MAGIC 
# MAGIC <b> Database: </b> real_time_iot_dashboard
# MAGIC 
# MAGIC <b> Tables: </b> bronze_sensors, silver_sensors, silver_sensors_stateful
# MAGIC 
# MAGIC <b> Params: </b> StartOver (Yes/No) - allows user to truncate and reload pipeline

# COMMAND ----------

# DBTITLE 1,Real-time Pipeline Architecture
# MAGIC %md
# MAGIC 
# MAGIC <img src="https://miro.medium.com/max/1400/0*o7m8qLauQ003jUu7">

# COMMAND ----------

# DBTITLE 1,Plotly Dash App Output
# MAGIC %md
# MAGIC 
# MAGIC <img src="https://miro.medium.com/max/1400/1*N2hJnle6RJ6HRRF4ISFBjw.gif">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Part 1: Raw to Bronze with Autoloader

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Get Started With Best Practices: 
# MAGIC 
# MAGIC <li> 1. Real-time Stateful Streaming with Watermarking: https://www.databricks.com/blog/2022/08/22/feature-deep-dive-watermarking-apache-spark-structured-streaming.html
# MAGIC   
# MAGIC <li> 2. Streaming Best Practices: https://docs.databricks.com/structured-streaming/production.html
# MAGIC   

# COMMAND ----------

# DBTITLE 1,Shuffle Partitions to Increase or Decrease Parallelism (2-4x cluster cores)
spark.conf.set("spark.sql.shuffle.partitions", "32")

# COMMAND ----------

# DBTITLE 1,2 Streaming Options: Autoloader for files, Kafka/Kinesis/EventHubs for Streaming Message Queues
# MAGIC %md
# MAGIC 
# MAGIC ## Autoloader Benefits: 
# MAGIC 
# MAGIC 1. More Scalable Directory Listing (incremental + file notification)
# MAGIC 2. Rocks DB State Store - Faster State management
# MAGIC 3. Schema Inference + Merge Schema: https://docs.databricks.com/ingestion/auto-loader/schema.html
# MAGIC 4. File Notification Mode - For ultra high file volumes from S3/ADLS/GCS: https://docs.databricks.com/ingestion/auto-loader/file-detection-modes.html
# MAGIC 5. Complex Sources -- advanced Glob Path Filters: <b> .option("pathGlobFilter", "[a-zA-Z].csv") </b> 
# MAGIC 6. Rescue data - automatically insert "bad" data into a rescued data column so you never lose data <b> .option("cloudFiles.rescuedDataColumn", "_rescued_data")</b>
# MAGIC 7. Flexible Schema Hints: <b> .option("cloudFiles.schemaHints", "tags map<string,string>, version int") </b> 
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

# DBTITLE 1,Input Parameters
dbutils.widgets.dropdown("StartOver", "Yes", ["No", "Yes"])
start_over = dbutils.widgets.get("StartOver")
print(f"Start Over?: {start_over}")

# COMMAND ----------

# DBTITLE 1,Do not edit - Standard file paths
## Output path of real-time data generator

file_source_location = "dbfs:/Filestore/real-time-data-demo/iot_dashboard/"
checkpoint_location = f"dbfs:/FileStore/real-time-data-demo-checkpoints/bronze"
database_name = "real_time_iot_dashboard"

## Other Params
checkpoint_location_stateful = f"dbfs:/FileStore/real-time-data-demo-checkpoints/silver_stateful"
checkpoint_location_silver = f"dbfs:/FileStore/real-time-data-demo-checkpoints/silver"
autoloader_schema_location = f"dbfs:/FileStore/real-time-data-demo-schema_checkpoints/AutoloaderDemoSchema/"

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid

# COMMAND ----------

# DBTITLE 1,Look at Raw Data Source
paths_df = spark.createDataFrame(dbutils.fs.ls(file_source_location))
display(paths_df.orderBy(desc(col("modificationTime"))))

# COMMAND ----------

# DBTITLE 1,Manually Generate Id Keys
#### Register udf for generating UUIDs
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

# DBTITLE 1,Create Database
spark.sql(f"""CREATE DATABASE IF NOT EXISTS {database_name};""")
##LOCATION 's3a://<path>' or 'adls://<path>'

# COMMAND ----------

# DBTITLE 1,Start Over - Truncate and Reload
if start_over == "Yes":
 
  spark.sql(f"""DROP TABLE IF EXISTS {database_name}.bronze_sensors""")
  spark.sql(f"""DROP TABLE IF EXISTS {database_name}.silver_sensors""")
  spark.sql(f"""DROP TABLE IF EXISTS {database_name}.silver_sensors_stateful""")
  spark.sql(f"""DROP TABLE IF EXISTS {database_name}.bronze_users""")
  spark.sql(f"""DROP TABLE IF EXISTS {database_name}.silver_users""")

# COMMAND ----------

# DBTITLE 1,Read Stream with Autoloader
## Why use autoloader? -- cloudFiles

df_raw = (spark
     .readStream
     .format("cloudFiles") ## csv, json, binary, text, parquet, avro
     .option("cloudFiles.format", "text")
     #.option("cloudFiles.useIncrementalListing", "true")
     #.option("cloudFiles.useNotifications", "true") !! IMPORTANT
     .option("cloudFiles.schemaLocation", autoloader_schema_location)
     #.option("schema", inputSchema)
     #.option("modifiedAfter", timestampString) ## option
     #.option("cloudFiles.maxFilesPerTrigger", 100000) ## maxBytesPerTrigger, 10mb
     #.option("pathGlobFilter", "*.json*") ## Only certain files ## regex expr
     .option("ignoreChanges", "true")
     #.option("ignoreDeletes", "true")
     #.option("latestFirst", "false")
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
#display(df_raw)

# COMMAND ----------

# DBTITLE 1,Delete Checkpoint Location to start over
if start_over == "Yes":
  dbutils.fs.rm(checkpoint_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,Write Stream -- same as any other stream
## with DBR 11.2+, insertion order clustering is automatic!

(df_raw
 .withColumn("date", date_trunc('day', col("timestamp")))
.writeStream
.format("delta")
.option("checkpointLocation", checkpoint_location)
.trigger(processingTime = '2 seconds') ## once=True, availableNow=True, processingTime = '5 minutes', continuous ='1 minute'
.partitionBy("date") ## DO NOT OVER PARTITION, but partition when you need to (data deletion for merge heavy tables, etc.)
.toTable(f"{database_name}.bronze_sensors") ## We do not need to define the DDL, it will be created on write, but we can define DDL if we want to
)

# COMMAND ----------

# DBTITLE 1,Watch the table update
## You can use this to see how often to trigger visuals directly in Dash!
history_df = spark.sql(f"""DESCRIBE HISTORY {database_name}.bronze_sensors;""")

display(history_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## LATENCY Can be affected by: 
# MAGIC 
# MAGIC <li> 1. How data comes in (kafka, files)
# MAGIC <li> 2. File listing (optimize your source files with incremental/file notifications)
# MAGIC <li> 3. Cluster Configs (node types)
# MAGIC <li> 4. Code logic
# MAGIC <li> 5. Partitioning
# MAGIC <li> 6. Spark Configs (like shuffle partitions)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Part 2: Streaming from Delta - with Stateful Aggregations

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Streaming With Delta: https://docs.databricks.com/structured-streaming/delta-lake.html#language-python

# COMMAND ----------

# DBTITLE 1,Define Target Silver Table
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE real_time_iot_dashboard.silver_sensors_stateful
# MAGIC (EventStart timestamp,
# MAGIC EventEnd timestamp,
# MAGIC EventWindow Struct<start:timestamp, end:timestamp>,
# MAGIC calories_burnt decimal(14,4),
# MAGIC miles_walked decimal(14,4),
# MAGIC num_steps decimal(14,4),
# MAGIC Date timestamp
# MAGIC )
# MAGIC PARTITIONED BY (Date)
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Configure Databricks to Automatically RocksDB to Management State Scalably
spark.conf.set("spark.sql.streaming.stateStore.providerClass","com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# COMMAND ----------

# DBTITLE 1,Adding Nuance - State Management / Late Arriving Data & Aggregations
## Most data pipelines for real-time need efficient state management
## This can add a bit of latency

df_bronze_stateful = (spark.readStream.format("delta")
  .option("withEventTimeOrder", "true") ## Ensure thats the even order processing is by event timestamp, not file modified timestamp
  #.option("maxFilesPerTrigger", 1)
  .table(f"{database_name}.bronze_sensors")
                      
  .withWatermark("timestamp", "30 seconds") ## Can only be 10 seconds late
  .groupBy(window("timestamp", "1 second").alias("EventWindow"))
     .agg(avg("calories_burnt").alias("calories_burnt"), 
          avg("miles_walked").alias("miles_walked"), 
          avg("num_steps").alias("num_steps"), 
         )
     #.withColumn("id", uuidUdf()) ## Adding Surrrogate keys is more helpful in EDW workloads, and its automatic in delta with IDENTITY :)
     .selectExpr("date_trunc('second', EventWindow.start) AS EventStart", 
                 "date_trunc('second', EventWindow.end) AS EventEnd",
                 "*", 
                 "date_trunc('day', EventWindow.start) AS Date")
                  )

# COMMAND ----------

# DBTITLE 1,Reprocess all data from Bronze
if start_over == "Yes":
  dbutils.fs.rm(checkpoint_location_stateful, recurse=True)

# COMMAND ----------

# DBTITLE 1,Write to Silver Stateful Table
## with DBR 11.2+, Order insertion clustering is automatic!

(df_bronze_stateful
.writeStream
.format("delta")
.outputMode("append")
.option("checkpointLocation", checkpoint_location_stateful)
.option("mergeSchema", "true")
.trigger(processingTime = '2 seconds') ## once=True, availableNow=True, processingTime = '5 minutes', continuous ='1 minute'
.partitionBy("Date") ## DO NOT OVER PARTITION, but partitions when you need to (data deletion, etc.)
.toTable(f"real_time_iot_dashboard.silver_sensors_stateful") ## We do not need to define the DDL, it will be created on write, but we can define DDL if we want to
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM real_time_iot_dashboard.silver_sensors_stateful

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <b> Q: Do we need to optimize our table with OPTIMZE/ZORDER? 
# MAGIC   
# MAGIC <li> A: If under 1 min SLA, probably not a good idea. Just partition on a date range and delete older data to keep table small

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Part 3: Streaming + MERGE for EDW Analytical Pipelines

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Now we have data streaming into a bronze table at any clip/rate we want. 
# MAGIC ## How can we stream into a silver table with merge?
# MAGIC 
# MAGIC <b> CAVEATS </b>
# MAGIC 
# MAGIC <li> 1. Only use merge when SLA is high enough
# MAGIC <li> 2. Merge is the most expensive operation in a database, do not use if you dont need it.
# MAGIC <li> 3. This design pattern is better suited for SLAs > 5s

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

# DBTITLE 1,Stream with Delta options
df_bronze_merge = (
  spark.readStream
#.option("startingVersion", "1") ## Or .option("startingTimestamp", "2018-10-18") You can optionally explicitly state which version to start streaming from
.option("ignoreChanges", "true") ## .option("ignoreDeletes", "true")
#.option("useChangeFeed", "true") ## Enables use change data feed
.option("maxFilesPerTrigger", 100) ## Optional - FIFO processing
.table(f"{database_name}.bronze_sensors")
)

#display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## If your data has updates and is not sub-second latency, you can use Structured Streaming to MERGE anything at any clip!

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
  
  
  """
  #### Python way to do the same thing
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
  
  
  return

# COMMAND ----------

# DBTITLE 1,Delete checkpoint - each stream has 1 checkpoint
dbutils.fs.rm(checkpoint_location_silver, recurse=True)

# COMMAND ----------

# DBTITLE 1,Write Stream as often as you want
(df_bronze_merge
.writeStream
.option("checkpointLocation", checkpoint_location_silver)
.trigger(processingTime='2 seconds') ## processingTime='1 minute' -- Now we can run this merge every minute!
.foreachBatch(mergeStatementForMicroBatch)
.start()
)

# COMMAND ----------

silver_df = spark.sql(f"""SELECT * FROM {database_name}.silver_sensors;""")

display(silver_df)
