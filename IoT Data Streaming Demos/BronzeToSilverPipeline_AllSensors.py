# Databricks notebook source
# MAGIC %md
# MAGIC <h1> Data Engineering Pipeline - Silver </h1>
# MAGIC 
# MAGIC <h2> Bronze to Silver: All Sensors Notebook</h2>
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
# MAGIC ### Stream 5: All Sensors Delta Table: Bronze --> Silver

# COMMAND ----------

##### Read file: Same output for either stream or static mode

### Stream # 5 for All Sensors

file_source_location_stream_5 = "/data/codydemos/bronze_allsensors/"
file_sink_location_stream_5 = "/data/codydemos/silver_allsensors/"
checkpoint_location_stream_5 = checkpoint_location + "AllSensors/"

if runMode == "Static":
  
  stream5_df = (spark
         .read
         .format("delta")
         .load(file_source_location_stream_5 + fileName)
        )
  
elif runMode == "Stream":
  
  stream5_df = (spark
     .readStream
     .format("delta")
     .load(file_source_location_stream_5) #table("bronze_allsensors")
    )

# COMMAND ----------

dbutils.fs.rm(file_sink_location_stream_5, recurse=True)

# COMMAND ----------

### Define silver table schema

silverAllSensorSchema = StructType([StructField("MeasurementDateTime", TimestampType(), True),
                                    StructField("SensorValue", DecimalType(), True),
                                    StructField("SensorUnitDescription", StringType(), True),
                                    StructField("SensorMeasurement", StringType(), True),
                                    StructField("SensorLocation", StringType(), True),
                                    StructField("Id", StringType(), False)]
                                     )


### create silver table if not exists

isAllSensorsTempThere = DeltaTable.isDeltaTable(spark, file_sink_location_stream_5)
print(f"Silver Table for All Sensors Exists: {isAllSensorsTempThere}")

if isAllSensorsTempThere == False:
  emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD(), silverAllSensorSchema)
  (emptyDF
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("SensorMeasurement")
    .option("path", file_sink_location_stream_5)
    .saveAsTable("silver_allsensors")
  )
  
  print("Created Empty Silver All Sensor Table for Stream 5!")

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Tip! 
# MAGIC 
# MAGIC To make foreachBatch indempotent delta supports the following:

# COMMAND ----------

"""
app_id = "XAFSDJVNSKDJBGKJGBSKJDFBSREKHJGBSSKf"

def writeToDeltaLakeTableIdempotent(batch_df, batch_id):
  batch_df.write.format(...).option("txnVersion", batch_id).option("txnAppId", app_id).save(...) # location 1
  batch_df.write.format(...).option("txnVersion", batch_id).option("txnAppId", app_id).save(...) # location 2
  
"""

# COMMAND ----------

##### This is the modelling logic that is the same regarding stream or file mode
##### Note: We can also do tons of things with Delta merges, and parallel processing, all can fit!

def stream5Controller(microBatchDf, BatchId):
  
  silverDeltaTable = DeltaTable.forPath(spark, file_sink_location_stream_5)
  
  """
  spark.sql("MERGE INTO target AS t
              USING source AS s ON t.Id = s.Id
            wheN MATCHEd... ")
            
  """
  
  (silverDeltaTable.alias("t")
  .merge(
    microBatchDf.alias("s"),
    "t.Id = s.Id"
        )
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute()
  )
  
  
  ##
  return

# COMMAND ----------

## Remove checkpoint for demo
## recurse = True, deletes all files inside a directory
dbutils.fs.rm(checkpoint_location_stream_5, recurse=True)

# COMMAND ----------

#### Actually execute stream or file run with same logic!

if runMode == "Static":
  
  stream5Controller(stream5_df, 1)
  
elif runMode == "Stream":
  
   (stream5_df
     .writeStream
     .trigger(once=True)
     .option("checkpointLocation", checkpoint_location_stream_5)
     .foreachBatch(stream5Controller)
     .start()
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM codydemos.silver_allsensors

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC OPTIMIZE codydemos.silver_allsensors
# MAGIC ZORDER BY (MeasurementDateTime)
