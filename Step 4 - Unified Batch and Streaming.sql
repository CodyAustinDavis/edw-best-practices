-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # This notebook generates a full data pipeline from databricks dataset - iot-stream using Autoloader + Structured Streaming
-- MAGIC 
-- MAGIC ## This creates 2 tables: 
-- MAGIC 
-- MAGIC <b> Database: </b> iot_dashboard
-- MAGIC 
-- MAGIC <b> Tables: </b> silver_sensors, silver_users 
-- MAGIC 
-- MAGIC <b> Params: </b> StartOver (Yes/No) - allows user to truncate and reload pipeline

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <img src="https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Bronze.png" >

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Autoloader Benefits: 
-- MAGIC 
-- MAGIC 1. More Scalable Directory Listing (incremental + file notification)
-- MAGIC 2. Rocks DB State Store - Faster State management
-- MAGIC 3. Schema Inference + Merge Schema: https://docs.databricks.com/ingestion/auto-loader/schema.html
-- MAGIC 4. File Notification Mode - For ultra high file volumes from S3/ADLS/GCS: https://docs.databricks.com/ingestion/auto-loader/file-detection-modes.html
-- MAGIC 5. Complex Sources -- advanced Glob Path Filters: <b> .option("pathGlobFilter", "[a-zA-Z].csv") </b> 
-- MAGIC 6. Rescue data - automatically insert "bad" data into a rescued data column so you never lose data <b> .option("cloudFiles.rescuedDataColumn", "_rescued_data")  </b>
-- MAGIC 7: Flexible Schema Hints: <b> .option("cloudFiles.schemaHints", "tags map<string,string>, version int") </b> 
-- MAGIC 
-- MAGIC Much more!
-- MAGIC 
-- MAGIC ### Auto loader intro: 
-- MAGIC https://docs.databricks.com/ingestion/auto-loader/index.html
-- MAGIC 
-- MAGIC Rescue Data: https://docs.databricks.com/ingestion/auto-loader/schema.html#rescue
-- MAGIC 
-- MAGIC 
-- MAGIC ### Auto Loader Full Options: 
-- MAGIC 
-- MAGIC https://docs.databricks.com/ingestion/auto-loader/options.html
-- MAGIC   
-- MAGIC ## Meta data options: 
-- MAGIC 
-- MAGIC Load the file metadata in auto loader for downstream continuity
-- MAGIC   https://docs.databricks.com/ingestion/file-metadata-column.html
-- MAGIC   

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC file_source_location = "dbfs:/databricks-datasets/iot-stream/data-device/"
-- MAGIC checkpoint_location = f"dbfs:/FileStore/shared_uploads/cody.davis@databricks.com/IotDemoCheckpoints/AutoloaderDemo/bronze"
-- MAGIC checkpoint_location_silver = f"dbfs:/FileStore/shared_uploads/cody.davis@databricks.com/IotDemoCheckpoints/AutoloaderDemo/silver"
-- MAGIC autoloader_schema_location = f"dbfs:/FileStore/shared_uploads/cody.davis@databricks.com/IotDemoCheckpoints/AutoloaderDemoSchema/"

-- COMMAND ----------

-- DBTITLE 1,Look at Raw Data Source
-- MAGIC %python 
-- MAGIC 
-- MAGIC dbutils.fs.ls('dbfs:/databricks-datasets/iot-stream/data-device/')

-- COMMAND ----------

-- DBTITLE 1,Imports
-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *

-- COMMAND ----------

-- DBTITLE 1,Create Database
CREATE DATABASE IF NOT EXISTS iot_dashboard_autoloader
--LOCATION 's3a://<path>'

-- COMMAND ----------

-- DBTITLE 1,Let Autoloader Sample files for schema inference - backfill interval can refresh this inference
-- MAGIC %python 
-- MAGIC ## for schema inference
-- MAGIC spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", 100) ## 1000 is default

-- COMMAND ----------

-- DBTITLE 1,Read Stream with Autoloader - LOTS of Options for any type of data
-- MAGIC %python
-- MAGIC 
-- MAGIC   df_raw = (spark
-- MAGIC      .readStream
-- MAGIC      .format("cloudFiles")
-- MAGIC      .option("cloudFiles.format", "json")
-- MAGIC      #.option("cloudFiles.useNotifications", "true")
-- MAGIC      .option("cloudFiles.schemaLocation", autoloader_schema_location)
-- MAGIC      #.option("schema", inputSchema)
-- MAGIC      #.option("modifiedAfter", timestampString) ## option
-- MAGIC      .option("cloudFiles.schemaHints", "calories_burnt FLOAT, timestamp TIMESTAMP")
-- MAGIC      .option("cloudFiles.maxFilesPerTrigger", 10) ## maxByesPerTrigger, 10mb
-- MAGIC      .option("pathGlobFilter", "*.json.gz") ## Only certain files ## regex expr
-- MAGIC      .load(file_source_location)
-- MAGIC      .select("*", "_metadata")
-- MAGIC      .withColumn("InputFileName", input_file_name())
-- MAGIC     )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(df_raw)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC ## delete checkpoint to start over
-- MAGIC 
-- MAGIC dbutils.fs.rm(checkpoint_location, recurse=True)

-- COMMAND ----------

-- DBTITLE 1,Write Stream -- same as any other stream
-- MAGIC %python
-- MAGIC 
-- MAGIC (df_raw
-- MAGIC .writeStream
-- MAGIC .format("delta")
-- MAGIC .option("checkpointLocation", checkpoint_location)
-- MAGIC .trigger(availableNow=True) ## once=True, processingTime = '5 minutes', continuous='1 minute'
-- MAGIC .toTable("iot_dashboard_autoloader.bronze_sensors")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ### Now we have data streaming into a bronze table at any clip/rate we want. 
-- MAGIC #### How can we stream into a silver table with merge?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <b> Stream options from a delta table: </b> https://docs.databricks.com/delta/delta-streaming.html
-- MAGIC 
-- MAGIC 
-- MAGIC <li> 1. Limit throughput rate: https://docs.databricks.com/delta/delta-streaming.html#limit-input-rate
-- MAGIC <li> 2. Specify starting version or timestamp: https://docs.databricks.com/delta/delta-streaming.html#specify-initial-position
-- MAGIC <li> 3. Ignore updates/deletes: https://docs.databricks.com/delta/delta-streaming.html#ignore-updates-and-deletes

-- COMMAND ----------

DESCRIBE HISTORY iot_dashboard_autoloader.bronze_sensors;

-- COMMAND ----------

-- DBTITLE 1,Stream with Delta options
-- MAGIC %python 
-- MAGIC 
-- MAGIC df_bronze = (spark.readStream
-- MAGIC .option("startingVersion", "1") ## Or .option("startingTimestamp", "2018-10-18")
-- MAGIC .option("ignoreChanges", "true") ## .option("ignoreDeletes", "true")
-- MAGIC .option("maxFilesPerTrigger", 100) ## Optional - FIFO processing
-- MAGIC .table("iot_dashboard_autoloader.bronze_sensors")
-- MAGIC )
-- MAGIC 
-- MAGIC #display(df_bronze)

-- COMMAND ----------

-- DBTITLE 1,Create Target Silver Table for Merge
CREATE OR REPLACE TABLE iot_dashboard_autoloader.silver_sensors
AS SELECT * FROM iot_dashboard_autoloader.bronze_sensors WHERE 1=2;

-- COMMAND ----------

-- DBTITLE 1,Define function to run on each microBatch (every 1s - every day, anything)
-- MAGIC %python 
-- MAGIC from delta.tables import *
-- MAGIC 
-- MAGIC def mergeStatementForMicroBatch(microBatchDf, microBatchId):
-- MAGIC   
-- MAGIC   silverDeltaTable = DeltaTable.forName(spark, "iot_dashboard_autoloader.silver_sensors")
-- MAGIC   
-- MAGIC   (silverDeltaTable.alias("target")
-- MAGIC   .merge(
-- MAGIC     microBatchDf.alias("updates"),
-- MAGIC     "target.Id = updates.Id AND updates.user_id = target.user_id AND target.device_id = updates.device_id"
-- MAGIC         )
-- MAGIC    .whenMatchedUpdate(set =
-- MAGIC     {
-- MAGIC       "calories_burnt": "updates.calories_burnt",
-- MAGIC       "miles_walked": "updates.miles_walked",
-- MAGIC       "num_steps": "updates.num_steps",
-- MAGIC       "timestamp": "updates.timestamp"
-- MAGIC     }
-- MAGIC   )
-- MAGIC   .whenNotMatchedInsertAll()
-- MAGIC   .execute()
-- MAGIC   )
-- MAGIC   
-- MAGIC   ## optimize table after the merge
-- MAGIC   
-- MAGIC   spark.sql("""OPTIMIZE iot_dashboard_autoloader.silver_sensors ZORDER BY (device_id)""")
-- MAGIC   
-- MAGIC   return

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm(checkpoint_location_silver, recurse=True)

-- COMMAND ----------

-- DBTITLE 1,Write Stream as often as you want
-- MAGIC %python
-- MAGIC 
-- MAGIC (df_bronze
-- MAGIC .writeStream
-- MAGIC #.mode("append") ## "complete", "update" 
-- MAGIC .option("checkpointLocation", checkpoint_location_silver)
-- MAGIC .trigger(availableNow=True) ## processingTime='1 minute' -- Now we can run this merge every minute!
-- MAGIC .foreachBatch(mergeStatementForMicroBatch)
-- MAGIC .start()
-- MAGIC )

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC SELECT * FROM iot_dashboard_autoloader.silver_sensors;
