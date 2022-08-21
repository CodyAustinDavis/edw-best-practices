# Databricks notebook source
# MAGIC %md
# MAGIC <h1> Data Engineering Pipeline - Silver </h1>
# MAGIC 
# MAGIC <h2> Bronze to Silver: Water Depth Notebook </h2>
# MAGIC 
# MAGIC <li> Stream from multiple sources in one notebook </li>
# MAGIC <li> Perform data Cleaning and ETL in forBatch design </li>
# MAGIC <li> Structured Streaming can use the same code, whether streaming or performing ad-hoc analysis. </li>
# MAGIC <li> Read a table, perform modelling, report on data in real time. </li>
# MAGIC <li> Debug and Develop with same code. </li>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Current Stage: Bronze --> Silver

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Bronze.png" >

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid
from pyspark.sql.functions import udf
from delta.tables import *

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS codydemos;
# MAGIC USE codydemos;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <b> Feature: </b> Add parameters to notebooks that jobs and other notebooks can use! Great for complex workflows or user friendly code!

# COMMAND ----------

##### Get Parameters for Notebook

dbutils.widgets.dropdown("Run Mode", "Stream", ["Static", "Stream"])
runMode = dbutils.widgets.get("Run Mode")

dbutils.widgets.text("File Name", "")
fileName = dbutils.widgets.get("File Name")

## Set up source and checkpoints

checkpoint_location = f"dbfs:/FileStore/shared_uploads/cody.davis@databricks.com/IotDemoCheckpoints/BronzeToSilver/"

print("Now running Bronze --> Silver Weather Data Streaming Service...")
print(f"Run Mode: {runMode}")

if runMode == "Static":
  print(f"Running file: {fileName}")

# COMMAND ----------

#### Register udf for generating UUIDs
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Notebook to stream multiple sources into multiple sinks

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Stream 1: Water Depth Delta Table: Bronze --> Silver

# COMMAND ----------

##### Read file: Same output for either stream or static mode

### Stream # 1 for Water depth

file_source_location_stream_1 = "/data/codydemos/bronze_waterdepthsensor/"
file_sink_location_stream_1 = "/data/codydemos/silver_waterdepthsensor/"
checkpoint_location_stream_1 = checkpoint_location + "WaterDepth/"

if runMode == "Static":
  
  stream1_df = (spark
         .read
         .format("delta")
         .load(file_source_location_stream_1 + fileName)
        )
  
elif runMode == "Stream":
  
  stream1_df = (spark
     .readStream
     .format("delta")
     .load(file_source_location_stream_1)
    )

# COMMAND ----------

dbutils.fs.rm(file_sink_location_stream_1, recurse=True)

# COMMAND ----------

### Define silver table schema

silverWaterDepthSchema = StructType([StructField("MeasurementDateTime", TimestampType(), True),
                                    StructField("SensorValue", DecimalType(), True),
                                    StructField("SensorUnitDescription", StringType(), True),
                                    StructField("SensorMeasurement", StringType(), True),
                                    StructField("SensorLocation", StringType(), True),
                                    StructField("Id", StringType(), False)]
                                     )


### create silver table if not exists

isSilverWaterDepthThere = DeltaTable.isDeltaTable(spark, file_sink_location_stream_1)
print(f"Silver Table for Water Depth Exists: {isSilverWaterDepthThere}")

if isSilverWaterDepthThere == False:
  emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD(), silverWaterDepthSchema)
  (emptyDF
    .write
    .format("delta")
    .mode("overwrite")
    .option("path", file_sink_location_stream_1)
    .saveAsTable("silver_waterdepthsensor")
  )
  
  print("Created Empty Silver Water Depth Table for Stream 1!")

# COMMAND ----------

##### This is the modelling logic that is the same regarding stream or file mode
##### Note: We can also do tons of things with Delta merges, and parallel processing, all can fit!

def stream1Controller(microBatchDf, BatchId):
  
  silverDeltaTable = DeltaTable.forPath(spark, file_sink_location_stream_1)
  
  (silverDeltaTable.alias("t")
  .merge(
    microBatchDf.alias("s"),
    "t.Id = s.Id"
        )
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute()
  )
  
  return

# COMMAND ----------

## Remove checkpoint for demo
## recurse = True, deletes all files inside a directory
dbutils.fs.rm(checkpoint_location_stream_1, recurse=True)

# COMMAND ----------

#### Actually execute stream or file run with same logic!

if runMode == "Static":
  
  stream1Controller(stream1_df, 1)
  
elif runMode == "Stream":
  
   (stream1_df
     .writeStream
     .trigger(once=True)
     .option("checkpointLocation", checkpoint_location_stream_1)
     .foreachBatch(stream1Controller)
     .start()
    )
