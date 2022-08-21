# Databricks notebook source
# DBTITLE 1,Raw to Kafka --> To Bronze
# MAGIC %md
# MAGIC 
# MAGIC <img src="https://i.pinimg.com/originals/e1/3f/67/e13f6703e4a52f2421ce4d5473604e40.png" width="400">
# MAGIC 
# MAGIC Docs: https://docs.databricks.com/spark/latest/structured-streaming/kafka.html#apache-kafka

# COMMAND ----------

# DBTITLE 1,Kafka Severs Config
kafka_bootstrap_servers_tls = "<your_kafka_bootstrap_server_tls>" "b-1.oetrta-kafka.oz8lgl.c3.kafka.us-west-2.amazonaws.com:9094,b-2.oetrta-kafka.oz8lgl.c3.kafka.us-west-2.amazonaws.com:9094"
kafka_bootstrap_servers_plaintext = "<your_kafka_bootstrap_server_plain_text>" "b-1.oetrta-kafka.oz8lgl.c3.kafka.us-west-2.amazonaws.com:9092,b-2.oetrta-kafka.oz8lgl.c3.kafka.us-west-2.amazonaws.com:9092"

# COMMAND ----------

# DBTITLE 1,Create your a Kafka topic unique to your name
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
# Short form of username, suitable for use as part of a topic name.
user = username.split("@")[0].replace(".","_")
topic = f"{user}_oetrta_kafka_test"

# COMMAND ----------

# DBTITLE 1,Get Streaming dataset
##### Get Parameters for Notebook

dbutils.widgets.dropdown("Run Mode", "Stream", ["Static", "Stream"])
runMode = dbutils.widgets.get("Run Mode")

dbutils.widgets.text("File Name", "")
fileName = dbutils.widgets.get("File Name")

## Set up source and checkpoints
file_source_location = f"dbfs:/FileStore/shared_uploads/cody.davis@databricks.com/IotDemo/"
checkpoint_location = f"dbfs:/FileStore/shared_uploads/cody.davis@databricks.com/IotDemoCheckpoints/RawToKafka/"

print( f"Running Kafka Producer for : {username}")
print( f" Streaming From: {file_source_location}")
print( f" Checkpoint Location: {checkpoint_location}")
print( f" To topic: {topic}")

if runMode == "Static":
  print(f"Running file: {fileName}")

# COMMAND ----------

# DBTITLE 1,Create UDF for UUID
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import random, string, uuid

uuidUdf= udf(lambda : uuid.uuid4().hex,StringType())

# COMMAND ----------

# DBTITLE 1,Define Source Input Schema
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
                                      StructField("Id", StringType(), True),
                                      StructField("EventDateTime", TimestampType(), True)]
                                     )

# COMMAND ----------

# DBTITLE 1,Loading streaming dataset and prepare for sending to Kafka
raw_input_stream = (spark
  .readStream
  .format("csv")
  .option("header", "true")
  .schema(weatherInputSensorSchema)
  .load(file_source_location)
  .filter((col("WindowAverageStartDateTime").isNotNull()) & (col("SensorValue").isNotNull())) 
  .drop("Skip", "SkipResult", "SkipTable", "WindowAverageStartDateTime", "WindowAverageStopDateTime")
  .withColumn("Id", uuidUdf())
  .withColumn("EventDateTime", current_timestamp())
                   )

#display(raw_input_stream)

# COMMAND ----------

# DBTITLE 1,Delete Checkpoint to reprocess all available data
dbutils.fs.rm(checkpoint_location, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Tip!
# MAGIC 
# MAGIC By default, when spark reads data from kafka, it creates 1:1 mapping of tasks for each partition on the topic, but you can increase the number of partitions and divide the data up from 1:X mappings

# COMMAND ----------

# DBTITLE 1,WriteStream to Kafka
# For the sake of an example, we will write to the Kafka servers using SSL/TLS encryption
# Hence, we have to set the kafka.security.protocol property to "SSL"

## .option("minPartitions", 100)
(raw_input_stream
   .select(col("Id").alias("key"), to_json(struct(col('MeasurementDateTime'), 
                                                       col('SensorValue'), 
                                                       col('SensorUnitDescription'), 
                                                       col('SensorMeasurement'), 
                                                       col('SensorLocation'), 
                                                       col('Id'),
                                                       col('EventDateTime'))
                                               ).alias("value")
          )
   .writeStream
   .format("kafka")
   .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls )
   .option("kafka.security.protocol", "SSL") 
   .option("checkpointLocation", checkpoint_location)
   .option("topic", topic)
   .start()
)

# COMMAND ----------

# DBTITLE 1,ReadStream from Kafka and Save to Bronze Stage 1 Delta Table
startingOffsets = "earliest"

## Define schema to unpack
input_schema = StructType([StructField("MeasurementDateTime", TimestampType(), True),
                                      StructField("SensorValue", DecimalType(), True),
                                      StructField("SensorUnitDescription", StringType(), True),
                                      StructField("SensorMeasurement", StringType(), True),
                                      StructField("SensorLocation", StringType(), True),
                                      StructField("Id", StringType(), True),
                                      StructField("EventDateTime", TimestampType(), True)]
                                     )

# In contrast to the Kafka write in the previous cell, when we read from Kafka we use the unencrypted endpoints.
# Thus, we omit the kafka.security.protocol property
kafka = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext ) 
  .option("subscribe", topic )
  .option("startingOffsets", startingOffsets )
  .load()
        )

read_stream = (kafka.select(col("key").cast("string").alias("topic"), col("value").alias("payload"))
              )

# COMMAND ----------

display(read_stream)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE codydemos;

# COMMAND ----------

dbutils.fs.rm("/data/codydemos/prebronze_allsensors", recurse=True)

# COMMAND ----------

display(read_stream)

# COMMAND ----------

# DBTITLE 1,Write Stream to Pre-Bronze Delta using Multiplexing Design Pattern

# For the sake of an example, we will write to the Kafka servers using SSL/TLS encryption
# Hence, we have to set the kafka.security.protocol property to "SSL"
(read_stream
   .writeStream
   .format("delta")
   .option("mode", "append")
   #.option("path",  "/data/codydemos/prebronze_allsensors") ##optional when using table method
   .option("checkpointLocation", checkpoint_location)
   #.option("maxFilesPerTrigger", 100)
   #.partitionBy("date")
   .table("PreBronzeAllSensorsFromKafka")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECt * FROM codydemos.PreBronzeAllSensorsFromKafka;
