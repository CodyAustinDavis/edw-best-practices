# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### This notebook simulates a real-time feed from an IoT Device
# MAGIC 
# MAGIC <b> Notes: </b>
# MAGIC   
# MAGIC   <li> 1. Starts with an initial batch of the earlist data from the databricks-datasets/iot-stream
# MAGIC   <li> 2. Allows user to truncate and reload simulated streaming data
# MAGIC   <li> 3. Allows user to decide how often to drop files to simulate different update frequencies

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "32")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Define Source and Sink Paths
source_data_path = "/databricks-datasets/iot-stream/data-device/"
target_data_path =  "dbfs:/Filestore/real-time-data-demo/iot_dashboard/"

# COMMAND ----------

# DBTITLE 1,Get all records, order by timestamp, and drop 1 at time
df = (spark.read.json(source_data_path).orderBy("timestamp")
      .withColumn("second", date_trunc("second", col("timestamp")))
     )

# COMMAND ----------

dbutils.widgets.text("Second Frequency (Integer)", "1")
dbutils.widgets.text("Starting Record Batch Size", "1000")
dbutils.widgets.dropdown("Start Over Each Run", "Yes", ["Yes", "No"])
dbutils.widgets.text("Records Per Trigger (Integer):", "1000")
dbutils.widgets.dropdown("Run Mode", "Real Time", ["Real Time", "Historical Stream"])

run_mode = dbutils.widgets.get("Run Mode")
start_over = dbutils.widgets.get("Start Over Each Run")
drop_periodicity = int(dbutils.widgets.get("Second Frequency (Integer)"))
start_batch_size = int(dbutils.widgets.get("Starting Record Batch Size"))
records_per_trigger = int(dbutils.widgets.get("Records Per Trigger (Integer):"))

print(f"Run Mode: {run_mode}... \n Generating {records_per_trigger} records every {drop_periodicity} seconds starting with {start_batch_size} records. \n Start over each run?: {start_over}")

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import *
import time

# COMMAND ----------

# DBTITLE 1,Sort Data to Drop files in order of timeframe to simulate real-time
historical_overSpec = Window.orderBy("timestamp")
realtime_overSpec = Window.orderBy("second")

prepped_df = (df.withColumn("row_num", row_number().over(historical_overSpec)) ## For 
              .withColumn("sec_rank", dense_rank().over(realtime_overSpec))
             )

# COMMAND ----------

# DBTITLE 1,Write Starting Batch to get initial state
## Start over each time 

if start_over == "Yes":
  print("Truncating and reloading source data...")
  dbutils.fs.rm(target_data_path, recurse=True)

  
## Write initial batch size

if run_mode == "Historical Stream":
  
  ## This separates data in batches by #rows
  initial_batch = prepped_df.filter(col("row_num") <= lit(start_batch_size)).select("value").coalesce(1)
  initial_batch.write.text(f"{target_data_path}initial_batch_0_{start_batch_size}.json")

elif run_mode == "Real Time":
  
  # This separates data in batches by seconds
  initial_batch = prepped_df.filter(col("sec_rank") <= lit(start_batch_size)).select("value").coalesce(1)
  initial_batch.write.text(f"{target_data_path}initial_batch_0_{start_batch_size}.json")

# COMMAND ----------

# DBTITLE 1,Load Incremental Records in order of timestamp after initial batch
if run_mode == "Historical Stream":
  
  max_val = prepped_df.agg(max("row_num")).collect()[0][0]
  batches = list(range(start_batch_size, max_val, records_per_trigger))


  coalesced_prepped_df = prepped_df.coalesce(1)

  for i, j in enumerate(batches):

    print(i)
    print(f"Dropping batch {i} from records {j} --> {batches[i+1]}")

    start_rec = j
    end_rec = batches[i+1]

    incremental_df = (coalesced_prepped_df
                    .filter((col("row_num") > lit(start_rec)) & (col("row_num") <= lit(end_rec)))
                    .coalesce(1)
                    .orderBy("row_num").select("value")
                   )
    incremental_df.write.text(f"{target_data_path}batch_{i}_from_{start_rec}_to_{end_rec}.json")

    time.sleep(drop_periodicity)
    
    
elif run_mode == "Real Time":
  
  max_val = prepped_df.agg(max("sec_rank")).collect()[0][0]
  
  ## Dropping X seconds of data at a time proportional to the real drop rate
  batches = list(range(start_batch_size, max_val, drop_periodicity))


  coalesced_prepped_df = prepped_df.coalesce(1)

  for i, j in enumerate(batches):

    print(i)
    print(f"Dropping batch {i} from records {j} --> {batches[i+1]}")

    start_rec = j
    end_rec = batches[i+1]

    incremental_df = (coalesced_prepped_df
                    .filter((col("sec_rank") > lit(start_rec)) & (col("sec_rank") <= lit(end_rec)))
                    .coalesce(1)
                    .orderBy("sec_rank").select("value")
                   )
    incremental_df.write.text(f"{target_data_path}batch_{i}_from_{start_rec}_to_{end_rec}.json")

    time.sleep(drop_periodicity)
