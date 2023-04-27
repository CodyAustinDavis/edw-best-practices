# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Controller notebook
# MAGIC
# MAGIC Identifies and Orcestrates the sub jobs

# COMMAND ----------

root_file_source_location = "dbfs:/databricks-datasets/iot-stream/data-device/"

# COMMAND ----------

import re
from pyspark.sql.functions import *
from pyspark.sql import Window, WindowSpec
from math import ceil as round_up

# COMMAND ----------

# DBTITLE 1,Logic to get event Name

## This can be manual and loaded from a config, or parsed out from data/metadata
@udf("string")
def get_event_name(input_string):
  return re.sub("part-", "", input_string.split(".")[0])

# COMMAND ----------

# DBTITLE 1,Define Load Balancing Parameters
tasks_per_cluster = 5
tasks_per_job = 10

# COMMAND ----------

spark.sql("""CREATE DATABASE IF NOT EXISTS iot_multiplexing_demo;""")

# COMMAND ----------

spark.sql("""CREATE TABLE IF NOT EXISTS iot_multiplexing_demo.job_orchestration_configs
(
JobCreationSetId BIGINT GENERATED BY DEFAULT AS IDENTITY,  
ParentJobName STRING,
JobName STRING,
event_names ARRAY<STRING>,
InputRootPath STRING,
JobCreationResult STRING
)
""")


# COMMAND ----------

# DBTITLE 1,Get Most Recent Max Job Set Id to keep creating new jobs
max_job_id = int(spark.sql("""SELECt MAX(JobCreationSetId) FROM iot_multiplexing_demo.job_orchestration_configs""").collect()[0][0])
print(max_job_id)

# COMMAND ----------

# DBTITLE 1,Step 1: Logic to get unique list of events/sub directories that separate the different streams
# Design considerations
# Ideally the writer of the raw data will separate out event types by folder so you can use globPathFilters to create separate streams
# If ALL events are in one data source, all streams will stream from 1 table and then will be filtered for that event in the stream. To avoid many file listings of the same file, enable useNotifications = true in autoloader


## This can be a stream source as well!!

## Get Active events with jobs that are already active (from previous runs)
df_active_event_jobs = spark.sql("""SELECT JobName, ParentJobName, explode(event_names) AS event_name
    FROM iot_multiplexing_demo.job_orchestration_configs""")


events_df = (spark.createDataFrame(dbutils.fs.ls(root_file_source_location))
            .withColumn("event_name", get_event_name(col("name")))
            .select("event_name")
            .distinct().alias("new_events")
            .join(df_active_event_jobs.alias("active_events"), on="event_name", how="left_anti") ## WHERE NOT IN but faster
)

display(events_df)

# COMMAND ----------

# DBTITLE 1,Parent Job Params
input_root_path = "dbfs:/databricks-datasets/iot-stream/data-device/"
parent_job_name = "parent_iot_stream"

# COMMAND ----------

# DBTITLE 1,Create Job DAGs in parallel with a spark udf - load balance first
    ## This can be inside a streaming foreach batch pipeline as well!
    
    events_balanced = (events_df
    .withColumn("ParentJobName", lit(parent_job_name))
    .withColumn("NumJobs", row_number().over(Window().orderBy(lit("1"))))
    .withColumn("JobGroup", ceil(col("NumJobs") / lit(tasks_per_job)) + lit(max_job_id)) ## Grouping tasks into Job Groups
    .withColumn("JobName", concat(col("ParentJobName"), lit("_"), col("JobGroup")))
    .groupBy(col("ParentJobName"), col("JobGroup"), col("JobName"))
    .agg(collect_list(col("event_name")).alias("event_names"))
    .withColumn("InputRootPath", lit(input_root_path))
    .selectExpr("JobGroup::bigint AS JobCreationSetId", "ParentJobName", "JobName", "event_names", "InputRootPath")
    )

# COMMAND ----------

# DBTITLE 1,Show Balanced Workload of Kobs
display(events_balanced)

# COMMAND ----------

# DBTITLE 1,Udf To Kick of A Job In Parallel
import re
import requests
import json
from math import ceil as round_up


@udf("string")
def build_streaming_job(job_name, input_root_path, parent_job_name, event_names, tasks_per_cluster):


  ## tasks_per_cluster not used in this example, but can be used to further refine shared cluster model
  full_job_name = parent_job_name + "_"+ job_name

  ## First Create the clusters based on tasks_per_cluster
  num_cluster_to_make = round_up(len(event_names)/tasks_per_cluster)
  clusters_to_create = [f"Job_Cluster_{str(i + 1)}" for i in range(0,num_cluster_to_make)]


  ## Optional, decide how many streams go onto each cluster or just group by job (in this example there will be 10 streams per job on 1 cluster)
  tasks = [{
            "task_key": f"event_{event}",
            "notebook_task": {
                "notebook_path": "/Repos/<user_name>/edw-best-practices/Advanced Notebooks/Multi-plexing with Autoloader/Option 1: Actually Multi-plexing tables on write/Child Job Template",
                "base_parameters": {
                    "Input Root Path": "dbfs:/databricks-datasets/iot-stream/data-device/",
                    "Parent Job Name": parent_job_name,
                    "Child Task Name": f"event_{event}"
                },
                "source": "WORKSPACE"
            },
            "job_cluster_key": str(clusters_to_create[round_up((i+1)/tasks_per_cluster) - 1]),
            "timeout_seconds": 0,
            "email_notifications": {}
            } for i, event in enumerate(event_names)
          ]


  ## Use jobs API to create a job for each grouping
  job_req = {
    "name": full_job_name,
    "email_notifications": {
        "no_alert_for_skipped_runs": "false"
    },
    "webhook_notifications": {},
    "timeout_seconds": 0,
    "schedule": {
      "quartz_cron_expression": "0 0 0 * * ?",
      "timezone_id": "UTC",
      "pause_status": "UNPAUSED"
  },
    "max_concurrent_runs": 1,
    "tasks": tasks,
    "job_clusters": [
        {
            "job_cluster_key": cluster,
            "new_cluster": {
                "cluster_name": "",
                "spark_version": "12.2.x-scala2.12",
                "aws_attributes": {
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK",
                    "zone_id": "us-west-2a",
                    "spot_bid_price_percent": 100,
                    "ebs_volume_count": 0
                },
                "node_type_id": "i3.xlarge",
                "enable_elastic_disk": "false",
                "data_security_mode": "SINGLE_USER",
                "runtime_engine": "STANDARD",
                "num_workers": 2
            }
        } for cluster in clusters_to_create
    ],
    "tags": {
      parent_job_name: ""
  },
    "format": "MULTI_TASK"
    }

  job_json = json.dumps(job_req)
  ## Get this from a secret or param
  dbx_token = "<dbx_token>"
  headers_auth = {"Authorization":f"Bearer {dbx_token}"}
  uri = "https://<workspace_host>/api/2.1/jobs/create"

  endp_resp = requests.post(uri, data=job_json, headers=headers_auth).json()
        
  ## For demo purposes, this just creates a job, but in PROD, you will want to add code to update an existing job for the job group if it already exists   
  return endp_resp

# COMMAND ----------

# DBTITLE 1,Define parallel execution
build_jobs_df = (events_balanced
  .withColumn("JobCreationResult", build_streaming_job(col("JobName"), col("InputRootPath"), col("ParentJobName"), col("event_names"), lit(tasks_per_cluster)))
)

# COMMAND ----------

# DBTITLE 1,Call action with collect or display or save
## This whole load balacing and job creations / running process can be inside a foreachBatch stream as well! all the same. 

build_jobs_df.write.format("delta").mode("append").saveAsTable("iot_multiplexing_demo.job_orchestration_configs")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM iot_multiplexing_demo.job_orchestration_configs;
