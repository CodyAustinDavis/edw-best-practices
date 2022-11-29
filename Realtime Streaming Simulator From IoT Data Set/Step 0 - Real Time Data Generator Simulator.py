# Databricks notebook source
# DBTITLE 1,Define Source and Sink Paths
source_data_path = "/databricks-datasets/iot-stream/data-device/"
target_data_path =  "dbfs:/Filestore/real-time-data-demo/iot_dashboard/"

# COMMAND ----------

# DBTITLE 1,Get all records, order by timestamp, and drop 1 at time
df = spark.read.json(source_data_path).orderBy("timestamp")

# COMMAND ----------

dbutils.widgets.text("Second Frequency (Integer)", "1")
dbutils.widgets.text("Starting Record Batch Size", "1000")
dbutils.widgets.dropdown("Start Over Each Run", "Yes", ["Yes", "No"])

start_over = dbutils.widgets.get("Start Over Each Run")
drop_periodicity = int(dbutils.widgets.get("Second Frequency (Integer)"))
start_batch_size = int(dbutils.widgets.get("Starting Record Batch Size"))

print(f"Generating Data Every {drop_periodicity} seconds... starting with {start_batch_size} records. \n Start over each run?: {start_over}")

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import *
import time

# COMMAND ----------

overSpec = Window.orderBy("timestamp")
prepped_df = df.withColumn("row_num", row_number().over(overSpec))

# COMMAND ----------

dbutils.fs.rm(f"dbfs:/Filestore/real-time-data-demo/iot_dashboard/", recurse=True)

# COMMAND ----------

# DBTITLE 1,Write Starting Batch to get initial state
## Start over each time 

if start_over == "Yes":
  print("Truncating and reloading source data...")
  dbutils.fs.rm("dbfs:/Filestore/real-time-data-demo/iot_dashboard/", recurse=True)

  
## Write initial batch size
initial_batch = prepped_df.filter(col("row_num") <= lit(start_batch_size)).select("value").coalesce(1)

initial_batch.write.text(f"dbfs:/Filestore/real-time-data-demo/iot_dashboard/initial_batch.json")


# COMMAND ----------

# DBTITLE 1,Load Incremental Records in order of timestamp after initial batch
incremental_df = prepped_df.filter(col("row_num") > lit(start_batch_size)).coalesce(1).orderBy("row_num").select("value", "row_num").collect()


for i, j in enumerate(incremental_df):
  
  print(j)
  rec = j[0]
  rec_name = f"rec_{i}"

  dbutils.fs.put(f"dbfs:/Filestore/real-time-data-demo/iot_dashboard/{rec_name}.json", rec, True)
  
  time.sleep(drop_periodicity)
