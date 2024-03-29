# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Controller notebook
# MAGIC
# MAGIC Identifies and Orcestrates the sub jobs

# COMMAND ----------

# DBTITLE 1,Parent Job Params
root_file_source_location = "dbfs:/databricks-datasets/iot-stream/data-device/"
input_root_path = "dbfs:/databricks-datasets/iot-stream/data-device/"
parent_job_name = "parent_iot_stream"

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
tasks_per_cluster = 3
tasks_per_job = 6

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
JobCreationResult STRING,
JobUpdateResult STRING
)
""")


# COMMAND ----------

# DBTITLE 1,Get Most Recent Max Job Set Id to keep creating new jobs
max_job_id = int(spark.sql("""SELECt MAX(JobCreationSetId) FROM iot_multiplexing_demo.job_orchestration_configs""").collect()[0][0] or 0)
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
            #.limit(16) ## Undo second time
            .join(df_active_event_jobs.alias("active_events"), on="event_name", how="left_anti") ## WHERE NOT IN but faster
            
).persist()

# COMMAND ----------

display(events_df)

# COMMAND ----------

# DBTITLE 1,Get list of events that need to be allocated to new or existing jobs
events_list = [i[0] for i in events_df.collect()]

print(events_list)

# COMMAND ----------

# DBTITLE 1,First allocate new events to non-full jobs
## This method ensures that overflow is allocated

active_not_full_jobs = (spark.sql(f"""SELECT substring(split(JobCreationResult, '=')[1], 1, length(split(JobCreationResult, '=')[1]) - 1)::bigint AS job_id, JobName, ParentJobName, event_names, {tasks_per_job} - array_size(event_names) AS remaining_slots,
    SUM({tasks_per_job} - array_size(event_names)) OVER(PARTITION BY ParentJobName ORDER BY JobName  DESC) AS total_allocated_slots
    FROM iot_multiplexing_demo.job_orchestration_configs
    WHERE array_size(event_names) < {tasks_per_job} 
    AND JobName != 'parent_iot_stream_4'
    """)
    .coalesce(1) ## this does not need to be parallelized
    .withColumn("new_events_list", lit(events_list).cast("array<string>"))
    .withColumn("allocated_slice", slice(col("new_events_list"), col("total_allocated_slots") - col("remaining_slots") + lit(1), col("remaining_slots")))
    .withColumn("new_event_names", array_union(col("event_names"), col("allocated_slice")))
)


display(active_not_full_jobs)

# COMMAND ----------

# DBTITLE 1,Get list of events allocated to EXISTING jobs and exclude from the jobs creation process
newly_allocated_events = [i[0] for i in active_not_full_jobs.selectExpr("explode(allocated_slice) AS distinct_allocated").distinct().collect()]

print(newly_allocated_events)

# COMMAND ----------

# DBTITLE 1,Get Remaining Newly Found Events that need NEW jobs created for them
events_for_new_jobs = events_df.filter(~col("event_name").isin(*newly_allocated_events))

display(events_for_new_jobs)

# COMMAND ----------

# DBTITLE 1,Create Job DAGs in parallel with a spark udf - load balance first
    ## This can be inside a streaming foreach batch pipeline as well!
    
    events_balanced = (events_for_new_jobs
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

# MAGIC %md
# MAGIC
# MAGIC # Two Jobs 
# MAGIC
# MAGIC 1. events_balanced - this dataframe contains all events that need to be allocated to NEW jobs
# MAGIC 2. active_not_full_jobs - this dataframe contains events that were allocated to existing jobs that are not yet at capacity. 

# COMMAND ----------

# DBTITLE 1,Udf To Kick of A Job In Parallel - for NEW job creation
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
                "notebook_path": "/Repos/cody.davis@databricks.com/edw-best-practices/Advanced Notebooks/Multi-plexing with Autoloader/Option 1: Actually Multi-plexing tables on write/Child Job Template",
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
  dbx_token = "<token>"
  headers_auth = {"Authorization":f"Bearer {dbx_token}"}
  uri = "https://<host_name>/api/2.1/jobs/create"

  endp_resp = requests.post(uri, data=job_json, headers=headers_auth).json()
        
  ## For demo purposes, this just creates a job, but in PROD, you will want to add code to update an existing job for the job group if it already exists   
  return endp_resp

# COMMAND ----------

# DBTITLE 1,Udf to update an existing job
## !! This is the laziest way to update a job - resetting the whole thing, but you can pass in the job id and just edits the tasks just as well this way
@udf("string")
def update_streaming_job(job_id, job_name, parent_job_name, event_names, tasks_per_cluster):


  ## tasks_per_cluster not used in this example, but can be used to further refine shared cluster model
  full_job_name = parent_job_name + "_"+ job_name

  ## First Create the clusters based on tasks_per_cluster
  num_cluster_to_make = round_up(len(event_names)/tasks_per_cluster)
  clusters_to_create = [f"Job_Cluster_{str(i + 1)}" for i in range(0,num_cluster_to_make)]


  ## Optional, decide how many streams go onto each cluster or just group by job (in this example there will be 10 streams per job on 1 cluster)
  tasks = [{
            "task_key": f"event_{event}",
            "notebook_task": {
                "notebook_path": "/Repos/cody.davis@databricks.com/edw-best-practices/Advanced Notebooks/Multi-plexing with Autoloader/Option 1: Actually Multi-plexing tables on write/Child Job Template",
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


  ## Use jobs API to Update a job that exists
  job_req = {
    "job_id": str(job_id),
    "new_settings": {
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
  }

  job_json = json.dumps(job_req)
  ## Get this from a secret or param
  dbx_token = "<token>"
  headers_auth = {"Authorization":f"Bearer {dbx_token}"}
  uri = "https://<host_name>/api/2.1/jobs/reset"

  endp_resp = requests.post(uri, data=job_json, headers=headers_auth).status_code
        
  ## For demo purposes, this just creates a job, but in PROD, you will want to add code to update an existing job for the job group if it already exists   
  return endp_resp

# COMMAND ----------

# DBTITLE 1,Define Execution Data Frame for NEW JOBS
build_jobs_df = (events_balanced
  .withColumn("JobCreationResult", build_streaming_job(col("JobName"), col("InputRootPath"), col("ParentJobName"), col("event_names"), lit(tasks_per_cluster)))
)

# COMMAND ----------


#display(updated_jobs)

# COMMAND ----------

# DBTITLE 1,Define Execution Data Frame for UPDATING NOT FILLED JOBS
updated_jobs = active_not_full_jobs.withColumn("JobUpdateResult", update_streaming_job(col("job_id"), col("JobName"), col("ParentJobName"), col("new_event_names"), lit(tasks_per_cluster)))


updated_jobs.createOrReplaceTempView("updated_jobs_final")

## This will trigger the job update API udf to run AND update the config
spark.sql("""
MERGE INTO iot_multiplexing_demo.job_orchestration_configs AS target
USING updated_jobs_final AS source
ON source.ParentJobName = target.ParentJobName
AND source.JobName = target.JobName
WHEN MATCHED THEN UPDATE SET 
target.event_names = source.new_event_names, 
target.JobUpdateResult = source.JobUpdateResult
""")

# COMMAND ----------

# DBTITLE 1,Call action with collect or display or save NEW JOBS
## This whole load balacing and job creations / running process can be inside a foreachBatch stream as well! all the same. 

build_jobs_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("iot_multiplexing_demo.job_orchestration_configs")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM iot_multiplexing_demo.job_orchestration_configs;
