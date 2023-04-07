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

# COMMAND ----------

# DBTITLE 1,Logic to get event Name

## This can be manual and loaded from a config, or parsed out from data/metadata
@udf("string")
def get_event_name(input_string):
  return re.sub("part-", "", input_string.split(".")[0])

# COMMAND ----------

# DBTITLE 1,Define Load Balancing Parameters
tasks_per_cluster = 2
tasks_per_job = 5

# COMMAND ----------

spark.sql("""CREATE DATABASE IF NOT EXISTS iot_multiplexing_demo;""")

# COMMAND ----------

# DBTITLE 1,Step 1: Logic to get unique list of events/sub directories that separate the different streams
# Design considerations
# Ideally the writer of the raw data will separate out event types by folder so you can use globPathFilters to create separate streams
# If ALL events are in one data source, all streams will stream from 1 table and then will be filtered for that event in the stream. To avoid many file listings of the same file, enable useNotifications = true in autoloader

events_df = (spark.createDataFrame(dbutils.fs.ls(root_file_source_location))
            .withColumn("event_name", get_event_name(col("name")))
            .select("event_name")
            .distinct()
)

display(events_df)

# COMMAND ----------

# DBTITLE 1,Parent Job Params
input_root_path = "dbfs:/databricks-datasets/iot-stream/data-device/"
parent_job_name = "parent_iot_stream"

# COMMAND ----------

# DBTITLE 1,Create Job DAGs in parallel with a spark udf - load balance first
events_balanced = (events_df
.withColumn("ParentJobName", lit(parent_job_name))
.withColumn("NumJobs", row_number().over(Window().orderBy(lit("1"))))
.withColumn("JobGroup", col("NumJobs") % lit(tasks_per_job)) ## Grouping tasks into Job Groups
.withColumn("JobName", concat(col("ParentJobName"), col("JobGroup")))
.groupBy(col("ParentJobName"), col("JobName"))
.agg(collect_list(col("event_name")).alias("event_names"))
.withColumn("InputRootPath", lit(input_root_path))
)

# COMMAND ----------

# DBTITLE 1,Show Balanced Workload of Kobs
display(events_balanced)

# COMMAND ----------

# DBTITLE 1,Udf To Kick of A Job In Parallel
import re
import requests
import json

@udf("string")
def build_streaming_job(job_name, input_root_path, parent_job_name, event_names, tasks_per_cluster):


  ## tasks_per_cluster not used in this example, but can be used to further refine shared cluster model
  full_job_name = parent_job_name + "_"+ job_name
  ## Optional, decide how many streams go onto each cluster or just group by job (in this example there will be 10 streams per job on 1 cluster)
  tasks = [{
            "task_key": f"event_{i}",
            "notebook_task": {
                "notebook_path": "/Repos/cody.davis@databricks.com/edw-best-practices/Advanced Notebooks/Multi-plexing with Autoloader/Option 1: Actually Multi-plexing tables on write/Child Job Template",
                "base_parameters": {
                    "Input Root Path": "dbfs:/databricks-datasets/iot-stream/data-device/",
                    "Parent Job Name": parent_job_name,
                    "Child Task Name": f"event_{i}"
                },
                "source": "WORKSPACE"
            },
            "job_cluster_key": "Job_cluster",
            "timeout_seconds": 0,
            "email_notifications": {}
            } for i in event_names
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
            "job_cluster_key": "Job_cluster",
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
                "num_workers": 4
            }
        }
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
  uri = "https://<workspace_url>/api/2.1/jobs/create"

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
build_jobs_df.write.format("delta").saveAsTable("iot_multiplexing_demo.job_orchestration_configs")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM iot_multiplexing_demo.job_orchestration_configs;
