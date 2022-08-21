# Databricks notebook source
# MAGIC %md
# MAGIC <h1> Data Engineering Pipeline - Silver </h1>
# MAGIC 
# MAGIC <h2> Bronze to Silver: Air Temp Notebook</h2>
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

checkpoint_location = f"<silver_checkpoint_location>"

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
# MAGIC ### Stream 2: Air Temp Delta Table: Bronze --> Silver

# COMMAND ----------

##### Read file: Same output for either stream or static mode

### Stream # 2 for Air Temp

file_source_location_stream_2 = "/data/codydemos/bronze_airtempsensor/"
file_sink_location_stream_2 = "/data/codydemos/silver_airtempsensor/"
checkpoint_location_stream_2 = checkpoint_location + "AirTemp/"

if runMode == "Static":
  
  stream2_df = (spark
         .read
         .format("delta")
         .load(file_source_location_stream_2 + fileName)
        )
  
elif runMode == "Stream":
  
  stream2_df = (spark
     .readStream
     .format("delta")
     .load(file_source_location_stream_2)
    )

# COMMAND ----------

dbutils.fs.rm(file_sink_location_stream_2, recurse=True)

# COMMAND ----------

### Define silver table schema

silverAirTempSchema = StructType([StructField("MeasurementDateTime", TimestampType(), True),
                                    StructField("SensorValue", DecimalType(), True),
                                    StructField("SensorUnitDescription", StringType(), True),
                                    StructField("SensorMeasurement", StringType(), True),
                                    StructField("SensorLocation", StringType(), True),
                                    StructField("Id", StringType(), False)]
                                     )


### create silver table if not exists

isSilverAirTempThere = DeltaTable.isDeltaTable(spark, file_sink_location_stream_2)
print(f"Silver Table for Air Temp Exists: {isSilverAirTempThere}")

if isSilverAirTempThere == False:
  emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD(), silverAirTempSchema)
  (emptyDF
    .write
    .format("delta")
    .mode("overwrite")
    .option("path", file_sink_location_stream_2)
    .saveAsTable("silver_airtempsensor")
  )
  
  print("Created Empty Silver Air Temp Table for Stream 2!")

# COMMAND ----------

##### This is the modelling logic that is the same regarding stream or file mode
##### Note: We can also do tons of things with Delta merges, and parallel processing, all can fit!

def stream2Controller(microBatchDf, BatchId):
  
  silverDeltaTable = DeltaTable.forPath(spark, file_sink_location_stream_2)

  #spark.sql("""MERGE INTO a....""")
  
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
dbutils.fs.rm(checkpoint_location_stream_2, recurse=True)

# COMMAND ----------

#### Actually execute stream or file run with same logic!

if runMode == "Static":
  
  stream2Controller(stream2_df, 1)
  
elif runMode == "Stream":
  
   (stream2_df
     .writeStream
     .trigger(once=True)
     .option("checkpointLocation", checkpoint_location_stream_2)
     .foreachBatch(stream2Controller)
     .start()
    )
