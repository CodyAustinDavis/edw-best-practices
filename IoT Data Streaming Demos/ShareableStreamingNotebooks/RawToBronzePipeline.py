# Databricks notebook source
# MAGIC %md
# MAGIC <h1> Data Engineering Pipeline </h1>
# MAGIC 
# MAGIC <h2> Structured Streaming With Sensor Data </h2>
# MAGIC 
# MAGIC <h3> Use Case: reading in raw climate sensor data from noaa using file based streaming </h3>
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

# DBTITLE 1,Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid
from pyspark.sql.functions import udf

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --DROP DATABASE IF EXISTS codydemos CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS codydemos;
# MAGIC USE codydemos;

# COMMAND ----------

# DBTITLE 1,Parameterize Notebook to Dynamically run in batch or streaming mode
##### Get Parameters for Notebook

dbutils.widgets.dropdown("Run Mode", "Stream", ["Static", "Stream"])
runMode = dbutils.widgets.get("Run Mode")

dbutils.widgets.text("File Name", "")
fileName = dbutils.widgets.get("File Name")

## Set up source and checkpoints
file_source_location = f"<file_source_path>"
checkpoint_location = f"<checkpoint_path>"

print("Now running Weather Data Streaming Service...")
print(f"...from source location {file_source_location}")
print(f"Run Mode: {runMode}")

if runMode == "Static":
  print(f"Running file: {fileName}")

# COMMAND ----------

#### Register udf for generating UUIDs
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Schema: Can be defined in separate directly and managed in Git for advanced schema evolution with Delta

# COMMAND ----------

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
# MAGIC ##### Read file: Same output for either stream or static mode

# COMMAND ----------

##### Read file: Same output for either stream or static mode

if runMode == "Static":
  
  df_raw = (spark
         .read
         .option("header", "true")
         .option("inferSchema", "true")
         .schema(weatherInputSensorSchema)
         .format("csv")
         .load(file_source_location + fileName)
         .withColumn("Id", uuidUdf())
        )
  
elif runMode == "Stream":
  
  ## To use Autoloader (incremental file listing, scalable file notification service, schema evolution, etc., use the format "cloudFiles")
  df_raw = (spark
     .readStream
     .format("csv") #cloudFiles
     .option("header", "true") # option("cloudFiles.format", "csv") 
     .schema(weatherInputSensorSchema)
     .load(file_source_location)
     .withColumn("Id", uuidUdf())
     .withColumn("InputFileName", input_file_name())
    )
    

# COMMAND ----------

#display(df_raw)

# COMMAND ----------

##### Do ETL/Modelling as needed for products

##### In this example we read 1 source of sensor data, and create a model of 5 delta tables that drive an analytics dashboard
df_cleaned = (df_raw
              .filter((col("WindowAverageStartDateTime").isNotNull()) & (col("SensorValue").isNotNull())) 
              .drop("Skip", "SkipResult", "SkipTable", "WindowAverageStartDateTime", "WindowAverageStopDateTime")
             )

# COMMAND ----------

#display(df_cleaned)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Stream one source to multiple sinks!

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://image.slidesharecdn.com/tathagatadas-191104200312/95/designing-etl-pipelines-with-structured-streaming-and-delta-lakehow-to-architect-things-right-40-638.jpg?cb=1572897862" >

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Perform Advacned ETL Logic inside the forEachBatch function

# COMMAND ----------

# DBTITLE 1,foreachBatch ETL Function (inputs: microBatchDf, batchId)
##### This is the modelling logic that is the same regarding stream or file mode
##### Note: We can also do tons of things with Delta merges, and parallel processing, all can fit!

def ModelSourceSensorData(microBatchDf, BatchId):
  
  #### If we want to make incremental, then we can insert merge statements here!!
  
  
  df_airTemp = (microBatchDf
                 .filter(col("SensorMeasurement")== lit("average_temperature"))
                  )
  
  df_waterQuality = (microBatchDf
                 .filter(col("SensorMeasurement")== lit("h2o_quality"))
                  )
  
  df_waterPH = (microBatchDf
                 .filter(col("SensorMeasurement")== lit("h2o_pH"))
                  )
  
  df_waterTemp = (microBatchDf
                 .filter(col("SensorMeasurement")== lit("h2o_temperature"))
                  )
  
  df_waterDepth = (microBatchDf
                 .filter(col("SensorMeasurement")== lit("h2o_depth"))
                  )
  
  ### Apply schemas
  
  ## Look up schema registry, check to see if the events in each subtype are equal to the most recently registered schema, Register new schema
  
  
  ##### Write to sink location
  microBatchDf.write.format("delta").option("mergeSchema", "true").option("path", "/data/codydemos/bronze_allsensors").mode("overwrite").saveAsTable("Bronze_AllSensors")
  df_waterDepth.write.format("delta").option("mergeSchema", "true").option("path", "/data/codydemos/bronze_waterdepthsensor").mode("overwrite").saveAsTable("Bronze_WaterDepthSensor")
  df_airTemp.write.format("delta").mode("overwrite").option("path", "/data/codydemos/bronze_airtempsensor").saveAsTable("Bronze_AverageAirTemperatureSensor")
  df_waterQuality.write.format("delta").mode("overwrite").option("path", "/data/codydemos/bronze_waterqualitysensor").saveAsTable("Bronze_WaterQualitySensor")
  df_waterPH.write.format("delta").mode("overwrite").option("path", "/data/codydemos/bronze_waterphsensor").saveAsTable("Bronze_WaterPhSensor")
  df_waterTemp.write.format("delta").mode("overwrite").option("path","/data/codydemos/bronze_watertemperaturesensor").saveAsTable("Bronze_WaterTemperatureSensor")
  
  return

# COMMAND ----------

## Remove checkpoint for demo
## recurse = True, deletes all files inside a directory
#dbutils.fs.rm(checkpoint_location, recurse=True)

# COMMAND ----------

#### Actually execute stream or file run with same logic!

if runMode == "Static":
  
  ModelSourceSensorData(df_cleaned, 1)
  
elif runMode == "Stream":
  ### Using For each batch - microBatchMode
   (df_cleaned
     .writeStream
     .trigger(once=True)
     .option("checkpointLocation", checkpoint_location)
     .foreachBatch(ModelSourceSensorData)
     .start()
   )

# COMMAND ----------

# DBTITLE 1,Standard Structured Streaming
### Just using standard structrued streaming
"""
(df_cleaned
 .writeStream
.trigger(once=True) #processingTime = '1m', continuous='30 seconds'
.option("checkpointLocation", checkpoint_location)
.option("path", "/data/codydemos/bronze_fullstreamfromkafka")
.table("BronzeFullStreamFromKafka")
)
"""

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC <h2> Overall Value: </h2>
# MAGIC 
# MAGIC <li> Highly scalable and easy to read pipelines </li>
# MAGIC <li> Multi language, same tech for any cloud platform </li>
# MAGIC <li> Read and write to almost any data source </li>
# MAGIC <li> Easy to build and debug </li>
# MAGIC <li> Logic is in one place for all teams to see, increasing speed, transparency, and knowledge transfer </li>
# MAGIC <li> Less code to write overall since streaming takes care of the incremental data checkpoint for you </li>
