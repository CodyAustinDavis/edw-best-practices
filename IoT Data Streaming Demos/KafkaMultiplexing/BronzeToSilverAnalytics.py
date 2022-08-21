# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # This notebook task the silver_allsensor tables, streams from that table, and summarizes the table as a streamed aggregate table with watermarking
# MAGIC 
# MAGIC ##Topics covered: 
# MAGIC 
# MAGIC <li> 1. Streaming Aggregations - groupBy, etc.
# MAGIC <li> 2. Handle late arriving data - Watermarking
# MAGIC <li> 3. Streaming Modes: Append, update, complete

# COMMAND ----------

# DBTITLE 1,Streaming Modes
# MAGIC %md
# MAGIC 
# MAGIC <b> Complete Mode </b> - The entire updated Result Table will be written to the external storage. It is up to the storage connector to decide how to handle writing of the entire table.
# MAGIC 
# MAGIC <b> Append Mode </b> - Only the new rows appended in the Result Table since the last trigger will be written to the external storage. This is applicable only on the queries where existing rows in the Result Table are not expected to change.
# MAGIC 
# MAGIC <b> Update Mode </b> -  Only the rows that were updated in the Result Table since the last trigger will be written to the external storage (available since Spark 2.1.1). Note that this is different from the Complete Mode in that this mode only outputs the rows that have changed since the last trigger. If the query doesn’t contain aggregations, it will be equivalent to Append mode.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE codydemos;

# COMMAND ----------

checkpoint_location = checkpoint_location = f"dbfs:/FileStore/shared_uploads/cody.davis@databricks.com/IotDemoCheckpoints/KafkaToSilver/"

# COMMAND ----------

df_silver = (spark
             .readStream
             .format("delta")
             .table("BronzeFullStreamFromKafka")
            )

# COMMAND ----------

display(df_silver)

# COMMAND ----------

# DBTITLE 1,Aggregations with streaming data frames
df_silver_agg = (df_silver
                 .groupBy("SensorLocation", "SensorMeasurement")
                 .agg(
                   avg("SensorValue").alias("AvgSensorValue"),
                   count("Id").alias("NumMeasurements"),
                   max("SensorValue").alias("HighestSensorValue"),
                   min("SensorValue").alias("LowestSensorValue")
                 )
                )
  
display(df_silver_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Types of windows: 
# MAGIC 
# MAGIC <img src="https://spark.apache.org/docs/latest/img/structured-streaming-time-window-types.jpg">

# COMMAND ----------

## Supplying 2 intervals in the window function creates a sliding window, 1 option creates a tumbling window, and then session window is separate

# COMMAND ----------

# DBTITLE 1,Window Aggregations for time-based streams
df_silver_agg = (df_silver
                 .groupBy("SensorLocation", "SensorMeasurement",
                          window("MeasurementDateTime", "10 minutes", "5 minutes"))
                 .agg(
                   avg("SensorValue").alias("AvgSensorValue"),
                   count("Id").alias("NumMeasurements"),
                   max("SensorValue").alias("HighestSensorValue"),
                   min("SensorValue").alias("LowestSensorValue")
                 )
                )
  
display(df_silver_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling Late Arriving Data
# MAGIC 
# MAGIC <img src="https://spark.apache.org/docs/latest/img/structured-streaming-watermark-update-mode.png">

# COMMAND ----------

# DBTITLE 1,Watermarking for late-arriving data
# Group the data by window and word and compute the count of each group

df_silver_agg = (df_silver
                 .withWatermark("MeasurementDateTime", "10 minutes") \
                 .groupBy("SensorLocation", "SensorMeasurement", window("MeasurementDateTime", "10 minutes", "5 minutes"))
                 .agg(
                   avg("SensorValue").alias("AvgSensorValue"),
                   count("Id").alias("NumMeasurements"),
                   max("SensorValue").alias("HighestSensorValue"),
                   min("SensorValue").alias("LowestSensorValue")
                 )
                )
  
display(df_silver_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## !! Watermarking forces the system to hold all data from the rolling time window in memory, so use it wisely

# COMMAND ----------

dbtuils.fs.rm("/data/codydemos/silver_fullstreamfromkafka", recurse=True)

# COMMAND ----------

# DBTITLE 1,Persist Aggregated Data to Table
(df_silver_agg
.writeStream
.mode("complete")
.format("delta")
.option("checkpointLocation", checkpoint_location)
.option("path", "/data/codydemos/silver_fullstreamfromkafka")
.table("SilverGroupedAggregate")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM SilverGroupedAggregate;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Other topics: 
# MAGIC 
# MAGIC ### Stream based joins:
# MAGIC 
# MAGIC 
# MAGIC Define watermark delays on both inputs such that the engine knows how delayed the input can be (similar to streaming aggregations)
# MAGIC 
# MAGIC Define a constraint on event-time across the two inputs such that the engine can figure out when old rows of one input is not going to be required (i.e. will not satisfy the time constraint) for matches with the other input. This constraint can be defined in one of the two ways.
# MAGIC 
# MAGIC Time range join conditions (e.g. ...JOIN ON leftTime BETWEEN rightTime AND rightTime + INTERVAL 1 HOUR),
# MAGIC 
# MAGIC Join on event-time windows (e.g. ...JOIN ON leftTimeWindow = rightTimeWindow).
# MAGIC 
# MAGIC 
# MAGIC """
# MAGIC from pyspark.sql.functions import expr
# MAGIC 
# MAGIC impressions = spark.readStream. ...
# MAGIC clicks = spark.readStream. ...
# MAGIC 
# MAGIC # Apply watermarks on event-time columns
# MAGIC impressionsWithWatermark = impressions.withWatermark("impressionTime", "2 hours")
# MAGIC clicksWithWatermark = clicks.withWatermark("clickTime", "3 hours")
# MAGIC 
# MAGIC # Join with event-time constraints
# MAGIC impressionsWithWatermark.join(
# MAGIC   clicksWithWatermark,
# MAGIC   expr("""
# MAGIC     clickAdId = impressionAdId AND
# MAGIC     clickTime >= impressionTime AND
# MAGIC     clickTime <= impressionTime + interval 1 hour
# MAGIC     """)
# MAGIC )
# MAGIC 
# MAGIC """
