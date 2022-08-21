# Databricks notebook source
# MAGIC %md
# MAGIC # Data Engineering Pipeline </h1>
# MAGIC 
# MAGIC <h2> Structured Streaming With Sensor Data </h2>
# MAGIC 
# MAGIC <li> Structured Streaming can use the same code, whether streaming or performing ad-hoc analysis. </li>
# MAGIC <li> Read a table, perform modelling, report on data in real time. </li>
# MAGIC <li> Debug and Develop with same code. </li>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Current Stage: Raw --> Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Bronze.png" >

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Configs

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid
from pyspark.sql.functions import udf

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP DATABASE IF EXISTS demos CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS demos;
# MAGIC USE demos;

# COMMAND ----------

# DBTITLE 1,Set up parameters
##### Get Parameters for Notebook

dbutils.widgets.dropdown("Run Mode", "Stream", ["Static", "Stream"])
runMode = dbutils.widgets.get("Run Mode")

dbutils.widgets.text("File Name", "")
fileName = dbutils.widgets.get("File Name")

## Set up source and checkpoints
## REPLACE WITH YOUR OWN PATHS
file_source_location = f"dbfs:/FileStore/shared_uploads/cody.davis@databricks.com/IotDemo/" #s3://codyaustindavisdemos/Demo/sales/"
checkpoint_location = f"dbfs:/FileStore/shared_uploads/cody.davis@databricks.com/IotDemoCheckpoints/RawToBronze2/"

print("Now running Weather Data Streaming Service...")
print(f"...from source location {file_source_location}")
print(f"Run Mode: {runMode}")

if runMode == "Static":
  print(f"Running file: {fileName}")

# COMMAND ----------

#### Register udf for generating UUIDs
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

## Dont necessarily need to define schema all the time. Better to define schema for speed, but schema-on-read is better just by specifying columns in a select or copy into statement
weatherInputSensorSchema = StructType([StructField("Skip", StringType(), True),
                                      StructField("SkipResult", StringType(), True),
                                      StructField("SkipTable", StringType(), True),
                                      StructField("WindowAverageStartDateTime", TimestampType(), True),
                                      StructField("WindowAverageStopDateTime", TimestampType(), True),
                                      StructField("MeasurementDateTime", TimestampType(), True),
                                      StructField("SensorValue", DecimalType(), True),
                                      StructField("SensorUnitDescription", StringType(), True),
                                      StructField("SensorMeasurement", StringType(), True),
                                      StructField("SensorLocation", StringType(), True),
                                      StructField("Id", StringType(), True)]
                                     )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Read Stream

# COMMAND ----------

##### Read file: Same output for either stream or static mode

if runMode == "Static":
  
  df_raw = (spark
         .read
         .option("header", "true")
         .option("inferSchema", "true")
         #.schema(weatherInputSensorSchema) #infer
         .format("csv")
         .load(file_source_location + fileName)
         .withColumn("Id", uuidUdf())
        )
  
elif runMode == "Stream":
  
  ## Cloudfiles is the highly scalable and efficient way to discover and manage schemas/raw files from S3
  df_raw = (spark
     .readStream
     .format("csv")
     .option("header", "true")
     .schema(weatherInputSensorSchema)
     .load(file_source_location)
     .withColumn("Id", uuidUdf()) ### Add some ETL columns that are helpful
     .withColumn("InputFileName", input_file_name())
    )

# COMMAND ----------

# DBTITLE 1,Do ETL As needed on the readStream or read
##### Do ETL/Modelling as needed for products

## Load model as pyfunc udf

##### In this example we read 1 source of sensor data, and create a model of 5 delta tables that drive an analytics dashboard
df_cleaned = (df_raw
              .filter((col("SensorValue").isNotNull())) 
              .drop("Skip", "SkipResult", "SkipTable")
#              .withColumn("predictions", model_udf(df))
             )

# COMMAND ----------

#display(df_cleaned)

# COMMAND ----------

# DBTITLE 1,Start over a stream -- deletes the memory of a stream job to reprocess files
## Remove checkpoint to restart

## recurse = True, deletes all files inside a directory
dbutils.fs.rm(checkpoint_location, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Write Stream

# COMMAND ----------

#### Actually execute stream or file run with same logic!

if runMode == "Static":
  
  df_cleaned.write.format("delta").option("path", "/data/codydemos/bronze_allsensors_simple_stream_2").saveAsTable("Bronze_AllSensors_Simple_2")
  
elif runMode == "Stream":
  ### Using For each batch - microBatchMode for abritrary sinks and complex transformations (groupings, one to many writes, etc.)
   (df_cleaned
     .writeStream
     .queryName("StreamIoT1")
     .trigger(once=True) #1. Continous - 1 minute, ProcessingTime='30 ', trigger=Once
     .option("checkpointLocation", checkpoint_location)
     .format("delta")
     .option("mergeSchema", "true")
     .option("path", "/data/codydemos/bronze_allsensors_simple_stream_2")
     .table("Bronze_AllSensors_Simple_2")
   )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Next Step: Merge Upsert

# COMMAND ----------

# DBTITLE 1,Create Silver Target Table
# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE Silver_AllSensors_Simple 
# MAGIC (
# MAGIC MeasurementDateTime TIMESTAMP,
# MAGIC SensorValue DECIMAL(10,0),
# MAGIC SensorUnitDescription STRING,
# MAGIC SensorMeasurement STRING,
# MAGIC SensorLocation STRING,
# MAGIC Id STRING,
# MAGIC InputFileName STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "/data/codydemos/silver_allsensors_simple_stream_2"
# MAGIC PARTITIONED BY (SensorLocation, SensorMeasurement);

# COMMAND ----------

# DBTITLE 1,Stream Bronze to Silver With Merge Using Delta
## This is an incremental load
bronze_all_sensors_df = spark.readStream.table("Bronze_AllSensors_Simple_2")

# COMMAND ----------

# DBTITLE 1,For each batch Logic - microBatch
def mergeFunction(inputDf, id):
  
  ##### SQL Version of Merge
  
  #from delta.tables import DeltaTable
  
  inputDf.createOrReplaceGlobalTempView("updates")
  
  spark.sql("""
  MERGE INTO Silver_AllSensors_Simple AS target
  USING global_temp.updates AS source --This can be a select statement that is incremental by timestamp like USING (SELECT * FROM updates WHERE update_timetstamp >= (SELECT MAX(update_timestamp) FROM SilverTable)
  ON target.Id = source.Id -- Can also add any other SCD logic
  WHEN NOT MATCHED
  THEN INSERT *
  """)
  
  
  ##### Python/Scala Version of Merge in Streaming
  """
  deltaTable = DeltaTable.forName("Silver_AllSensors_Simple")
  
  (deltaTable.alias("t")
    .merge(
      inputDf.alias("s"),
      "s.Id = t.Id")
    .whenMatched().updateAll()
    .whenNotMatched().insertAll()
    .execute()
  )
  """
  return

# COMMAND ----------

# DBTITLE 1,Checkpoint path defined by user - where you want the streams "memory" to live -- this is how you clear the memory
dbutils.fs.rm("dbfs:/FileStore/shared_uploads/cody.davis@databricks.com/IotDemoCheckpoints/BronzeToSilverSimple_2/")

# COMMAND ----------

(bronze_all_sensors_df
 .writeStream
 .queryName("IotStreamUpsert")
 .trigger(once=True) #processingTime='15 seconds' 
 .option("checkpointLocation", f"dbfs:/FileStore/shared_uploads/cody.davis@databricks.com/IotDemoCheckpoints/BronzeToSilverSimple_2/")
 .foreachBatch(mergeFunction)
 .start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM Silver_AllSensors_Simple

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC OPTIMIZE Silver_AllSensors_Simple ZORDER BY (MeasurementDateTime)
