# Databricks notebook source
# MAGIC %md
# MAGIC <h1> Data Engineering Pipeline </h1>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Current Stage: Kafka --> Bronze

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

# DBTITLE 1,Get Parameters for Job Set up
##### Get Parameters for Notebook

dbutils.widgets.dropdown("Run Mode", "Stream", ["Static", "Stream"])
runMode = dbutils.widgets.get("Run Mode")

dbutils.widgets.text("File Name", "")
fileName = dbutils.widgets.get("File Name")

## Set up source and checkpoints
checkpoint_location = f"dbfs:/FileStore/shared_uploads/cody.davis@databricks.com/IotDemoCheckpoints/KafkaToBronze/"

print("Now running Weather Data Streaming Service...")
print(f"Run Mode: {runMode}")

# COMMAND ----------

#### Register udf for generating UUIDs
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Schema: Can be defined in separate directly and managed in Git/glue/schema registry for advanced schema evolution with Delta

# COMMAND ----------

weatherInputSensorSchema = StructType([StructField("MeasurementDateTime", TimestampType(), True),
                                      StructField("SensorValue", DecimalType(), True),
                                      StructField("SensorUnitDescription", StringType(), True),
                                      StructField("SensorMeasurement", StringType(), True),
                                      StructField("SensorLocation", StringType(), True),
                                      StructField("Id", StringType(), True),
                                      StructField("EventDateTime", TimestampType(), True)]
                                     )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Read file: Same output for either stream or static mode

# COMMAND ----------

##### Read file: Same output for either stream or static mode

if runMode == "Static":
  
  df_bronze = (spark
         .read
         .format("delta")
         .table("PreBronzeAllSensorsFromKafka")
        )
  
elif runMode == "Stream":
  
  df_bronze = (spark
     .readStream
     .format("delta")
     .table("PreBronzeAllSensorsFromKafka")
    )
    

# COMMAND ----------

# DBTITLE 1,Check data we are getting from streaming data source
display(df_bronze)

# COMMAND ----------

# DBTITLE 1,Unpack data source into schema
# If json column is string, convert to struct: , otherwise, just read!
#from_json(col("value"), weatherInputSensorSchema).alias("values") 

df_cleaned = (df_bronze
              .select("value.*")
             )

# COMMAND ----------

# DBTITLE 1,Check unpacked cleaned up data
display(df_cleaned)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Stream one source to multiple sinks!

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --DROP DATABASE IF EXISTS codydemos CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS codydemos;
# MAGIC USE codydemos;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img src="https://image.slidesharecdn.com/tathagatadas-191104200312/95/designing-etl-pipelines-with-structured-streaming-and-delta-lakehow-to-architect-things-right-40-638.jpg?cb=1572897862" >

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Perform Advacned ETL Logic inside the forEachBatch function

# COMMAND ----------

##### This is the modelling logic that is the same regarding stream or file mode
##### Note: We can also do tons of things with Delta merges, and parallel processing, all can fit!

def ModelSourceSensorData(microBatchDf, BatchId):
  
  
  #### 
  #### If we want to make incremental, then we can insert merge statements here!!
  
  df_waterDepth = (microBatchDf
                 .filter(col("SensorMeasurement") == lit("h2o_feet"))
                  )
  
  df_airTemp = (microBatchDf
                 .filter(col("SensorMeasurement") == lit("average_temperature"))
                  )
  
  df_waterQuality = (microBatchDf
                 .filter(col("SensorMeasurement") == lit("h2o_quality"))
                  )
  
  df_waterPH = (microBatchDf
                 .filter(col("SensorMeasurement")== lit("h2o_pH"))
                  )
  
  df_waterTemp = (microBatchDf
                 .filter(col("SensorMeasurement")== lit("h2o_temperature"))
                  )
  
  
  ###
  
  
  ##### Write to sink location
  microBatchDf.write.format("delta").option("mergeSchema", "true").option("path", "/data/codydemos/bronze_allsensors").mode("append").saveAsTable("Bronze_AllSensors")
  df_waterDepth.write.format("delta").option("mergeSchema", "true").option("path", "/data/codydemos/bronze_waterdepthsensor").mode("append").saveAsTable("Bronze_WaterDepthSensor")
  df_airTemp.write.format("delta").mode("append").option("path", "/data/codydemos/bronze_airtempsensor").saveAsTable("Bronze_AverageAirTemperatureSensor")
  df_waterQuality.write.format("delta").mode("append").option("path", "/data/codydemos/bronze_waterqualitysensor").saveAsTable("Bronze_WaterQualitySensor")
  df_waterPH.write.format("delta").mode("append").option("path", "/data/codydemos/bronze_waterphsensor").saveAsTable("Bronze_WaterPhSensor")
  df_waterTemp.write.format("delta").mode("append").option("path","/data/codydemos/bronze_watertemperaturesensor").saveAsTable("Bronze_WaterTemperatureSensor")
  
  return

# COMMAND ----------

dbutils.fs.rm("/data/codydemos/bronze_allsensors", recurse=True)
dbutils.fs.rm("/data/codydemos/bronze_waterdepthsensor", recurse=True)
dbutils.fs.rm("/data/codydemos/bronze_airtempsensor", recurse=True)
dbutils.fs.rm("/data/codydemos/bronze_waterqualitysensor", recurse=True)
dbutils.fs.rm("/data/codydemos/bronze_waterphsensor", recurse=True)
dbutils.fs.rm("/data/codydemos/bronze_watertemperaturesensor", recurse=True)
dbutils.fs.rm("/data/codydemos/bronze_fullstreamfromkafka", recurse=True)

# COMMAND ----------

## Remove checkpoint for demo
## recurse = True, deletes all files inside a directory
dbutils.fs.rm(checkpoint_location, recurse=True)

# COMMAND ----------

# DBTITLE 1,foreach/forEachBatch mode
#### Actually execute stream or file run with same logic!

if runMode == "Static":
  
  ModelSourceSensorData(df_cleaned, 1)
  
elif runMode == "Stream":
  
   (df_cleaned
     .writeStream
     .trigger(once=True)
     .option("checkpointLocation", checkpoint_location)
     .foreachBatch(ModelSourceSensorData)
     .start()
    )

# COMMAND ----------

# DBTITLE 1,If we do not need to write this stream to multiple tables, we can simply run the stream normally
if runMode == "Static":
  
  (df_cleaned
   .write
   .format("delta")
   .mode("overwrite") #overwrite
   .option("path", "/data/codydemos/bronze_fullstreamfromkafka")
   .saveAsTable("BronzeFullStreamFromKafka")
  )
   
  
elif runMode == "Stream":

  (df_cleaned
  .writeStream
  .format("delta")
  .trigger(once=True) #processingTime = '1m', continuous='30 seconds'
  .option("checkpointLocation", checkpoint_location)
  .option("path", "/data/codydemos/bronze_fullstreamfromkafka")
  .table("BronzeFullStreamFromKafka")
  )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Trigger options: default, continous, once, processing time interval
# MAGIC 
# MAGIC # Default trigger (runs micro-batch as soon as it can)
# MAGIC df.writeStream \
# MAGIC   .format("console") \
# MAGIC   .start()
# MAGIC 
# MAGIC # ProcessingTime trigger with two-seconds micro-batch interval
# MAGIC df.writeStream \
# MAGIC   .format("console") \
# MAGIC   .trigger(processingTime='2 seconds') \
# MAGIC   .start()
# MAGIC 
# MAGIC # One-time trigger
# MAGIC df.writeStream \
# MAGIC   .format("console") \
# MAGIC   .trigger(once=True) \
# MAGIC   .start()
# MAGIC 
# MAGIC # Continuous trigger with one-second checkpointing interval
# MAGIC df.writeStream
# MAGIC   .format("console")
# MAGIC   .trigger(continuous='1 second')
# MAGIC   .start()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Processing Speeds: 
# MAGIC 
# MAGIC ### Continous: ~1 ms latency
# MAGIC 
# MAGIC ### microBatch: ~100 ms latency

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM BronzeFullStreamFromKafka;
