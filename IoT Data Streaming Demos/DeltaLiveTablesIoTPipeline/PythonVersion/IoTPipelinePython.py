# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # IoT Streaming Pipeline with Delta Live Tables
# MAGIC 
# MAGIC ## NOAA Climate Sensor Data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Imports and UDFs

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

import dlt

# COMMAND ----------

#### Register udf for generating UUIDs
import uuid
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Configs

# COMMAND ----------

# DBTITLE 1,Define Tables Config
## Load from a file
#config_str = spark.read.format("text").option("multiLine", "true").load("s3://oetrta/codyaustindavis/deltalivetables/dlt_config.json").limit(1).collect()[0][0]
#config = json.loads(config_str)

## Add trigger, partition, and zorder configs
config = {"RawToBronze": {"air_temp": "average_temperature", "water_ph": "h2o_pH", "water_quality": "h2o_quality", "water_temp": "h2o_temperature"},
          "BronzeToSilver": {"air_temp": {"mergeKey":["Id"], "sequenceBy": "MeasurementDateTime"},
                             "water_ph": {"mergeKey":["Id"], "sequenceBy": "MeasurementDateTime"},
                             "water_quality": {"mergeKey":["Id"], "sequenceBy": "MeasurementDateTime"},
                             "water_temp": {"mergeKey":["Id"], "sequenceBy": "MeasurementDateTime"}
                             }
         }

print(config)

# COMMAND ----------

# DBTITLE 1,Schemas - Can live in Config file
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

waterBronzeSensorSchema = StructType([StructField("MeasurementDateTime", TimestampType(), True),
                                    StructField("SensorValue", DecimalType(), True),
                                    StructField("SensorUnitDescription", StringType(), True),
                                    StructField("SensorMeasurement", StringType(), True),
                                    StructField("SensorLocation", StringType(), True),
                                    StructField("Id", StringType(), False),
                                     StructField("InputFileName", StringType(), False)]
                                     )

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Raw to Bronze

# COMMAND ----------

# DBTITLE 1,Step 1: Raw to Bronze
@dlt.table(
  name='Bronze_IoT_AllSensors',
  comment='This is the bronze table for all IoT sensor data coming from NOAA',
  table_properties ={'quality':'bronze'},
  schema=waterBronzeSensorSchema)
@dlt.expect_all_or_drop({"MeasurementDateTime":"MeasurementDateTime IS NOT NULL","SensorValue":"SensorValue IS NOT NULL"})
#@dlt.expect_all_or_fail({"MeasurementDateTime":"MeasurementDateTime IS NOT NULL","SensorValue":"SensorValue IS NOT NULL"})
#@dlt.expect_all({"MeasurementDateTime":"MeasurementDateTime IS NOT NULL","SensorValue":"SensorValue IS NOT NULL"})
def read_raw_data():
  
  data_source = spark.conf.get("iotpipeline.data_source_path")
  
  df = (spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(weatherInputSensorSchema)
        .load(data_source)
        .withColumn("Id", uuidUdf())
        .withColumn("InputFileName", input_file_name())
        .drop("Skip", "SkipResult", "SkipTable", "WindowAverageStartDateTime", "WindowAverageStopDateTime")
       )

  ## From Kafka Instead
  """
    df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext ) 
    .option("subscribe", topic )
    .option("startingOffsets", startingOffsets )
    .load()
          )

  read_stream = (kafka.select(col("key").cast("string").alias("topic"), col("value").alias("payload"))
                )
                """
  return df

# COMMAND ----------

# DBTITLE 1,Load Configs
raw_to_bronze_config = config.get("RawToBronze")
bronze_to_silver_config = config.get("BronzeToSilver")
print(f"Raw To Bronze: {raw_to_bronze_config}")
print(f"Bronze To Silver: {bronze_to_silver_config}")

# COMMAND ----------

# DBTITLE 1,Step 2: Bronze to Individual Tables (Still Bronze Quality)
### Use meta-programming model

def generate_tables(call_table, filter):
  @dlt.table(
    name=call_table,
    comment=f"Bronze Table {call_table} By Sensor"
  )
  def create_call_table():
    
    df = (dlt.read_stream("Bronze_IoT_AllSensors")
          .filter(col("SensorMeasurement") == lit(filter))
         )
    
    return df

# COMMAND ----------

# DBTITLE 1,Programmatically generate multiple downstream tables using table/schema registry
for table in raw_to_bronze_config.keys():
      
      table_name = f"Bronze_{table}"
      print(f"Generating Table {table_name}")
      generate_tables(table_name, raw_to_bronze_config.get(table))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Pro Tips!
# MAGIC 
# MAGIC <li> 1. You can combine dynamic table generation with config based expectations to pass in dynamic expectations for each dynamic table, etc. 
# MAGIC <li> 2. When merge is supported, you can even dynamically store merge logic and just insert into the a merge wrapper (on="key", update = {}, insert={}, source_name, target_name)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Bronze To Silver

# COMMAND ----------

# DBTITLE 1,Step 3: Silver Aggregate Table for each bronze table
### This feature is not available for private preview yet and is only avilable in SQL DLT using APPLY CHANGES for about 1 more month as of 1/2022

def generate_silver_tables(target_table, source_table, merge_keys, sequence_key):
  
  dlt.create_target_table(
  name = target_table,
  comment = "Silver Table"
  #spark_conf={"<key>" : "<value", "<key" : "<value>"},
  #table_properties={"<key>" : "<value>", "<key>" : "<value>"},
  #partition_cols=["<partition-column>", "<partition-column>"],
  #path="<storage-location-path>",
  #schema="schema-definition"
  )
    
  dlt.apply_changes(
    target = target_table,
    source = source_table,
    keys = merge_keys,
    sequence_by = sequence_key,
    ignore_null_updates = False,
    apply_as_deletes = None,
    column_list = None,
    except_column_list = None
    )
    
  return

# COMMAND ----------

# DBTITLE 1,Merging Delta Tables in DLT - CDC
for table in bronze_to_silver_config.keys():
    source_table_name = f"Bronze_{table}"
    target_table_name = f"Silver_{table}"
    merge_keys = bronze_to_silver_config.get(table).get("mergeKey")
    sequence_key = bronze_to_silver_config.get(table).get("sequenceBy")
    generate_silver_tables(target_table_name, source_table_name, merge_keys, sequence_key)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Silver to Gold

# COMMAND ----------

# DBTITLE 1,Step 4: Create Aggregated Gold Tables
@dlt.table(
name='Gold_AllSensorAggregates',
comment='Gold Table Sensor All Time Aggregates',
)
@dlt.expect_all_or_drop({'SensorLocation':'SensorLocation IS NOT NULL', 
                         'SensorMeasurement':'SensorMeasurement IS NOT NULL'
                        }
                       )
def create_gold_aggregates():
  
  df_gold_agg_all = (dlt.read("Bronze_IoT_AllSensors")
                   .groupBy("SensorLocation", "SensorMeasurement")
                   .agg(
                     avg("SensorValue").alias("AvgSensorValue"),
                     count("Id").alias("NumMeasurements"),
                     max("SensorValue").alias("HighestSensorValue"),
                     min("SensorValue").alias("LowestSensorValue")
                   )
                  )
  return df_gold_agg_all

# COMMAND ----------

# DBTITLE 1,Step 5: Create Windowed Gold Tables
@dlt.table(
name='Gold_AllSensorWindowedAggregates',
comment='Gold Table Sensor All Time Windowed Aggregates',
)
@dlt.expect_all_or_drop({'SensorLocation':'SensorLocation IS NOT NULL', 
                         'SensorMeasurement':'SensorMeasurement IS NOT NULL'
                        })
def create_gold_aggregates():
  
  df_gold_agg_windowed = (dlt.read_stream("Bronze_IoT_AllSensors")
                   .groupBy("SensorLocation", "SensorMeasurement", window("MeasurementDateTime", "10 minutes", "5 minutes"))
                   .agg(
                     avg("SensorValue").alias("AvgSensorValue"),
                     count("Id").alias("NumMeasurements"),
                     max("SensorValue").alias("HighestSensorValue"),
                     min("SensorValue").alias("LowestSensorValue")
                   )
                  )
  return df_gold_agg_windowed

# COMMAND ----------

# DBTITLE 1,Step 6: Create persisted complete tables for each sensor table do to summary statistics
def generate_sensor_gold_tables(table):
  source_table_name = f"Silver_{table}"
  target_table_name = f"Gold_{table}Aggregates"
  @dlt.table(
    name=target_table_name,
    comment= f"Gold Table By Sensor for {table}"
  )
  def create_call_table():
    
    df_gold_agg_windowed = (dlt.read(source_table_name)
                   .groupBy("SensorLocation", "SensorMeasurement")
                   .agg(
                     avg("SensorValue").alias("AvgSensorValue"),
                     count("Id").alias("NumMeasurements"),
                     max("SensorValue").alias("HighestSensorValue"),
                     min("SensorValue").alias("LowestSensorValue")
                   )
                  )
    
    
    
    return df_gold_agg_windowed

# COMMAND ----------

# DBTITLE 1,Dynamically Build All Summary Tables for Each Sensor
for table in bronze_to_silver_config.keys():
  generate_sensor_gold_tables(table)
