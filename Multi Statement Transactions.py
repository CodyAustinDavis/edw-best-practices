# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## This notebook provides functionality for multi-statement transactions -- pass in the SQL transaction and the class will make sure all your tables are rolled back if any in the statement fail

# COMMAND ----------

# MAGIC %pip install sqlparse
# MAGIC %pip install sql-metadata

# COMMAND ----------

import json
import sqlparse
from sql_metadata import Parser
import requests
import re
import os
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

import json


raw_session = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())

# COMMAND ----------

from datetime import datetime, timedelta

# COMMAND ----------

print(raw_session.get('tags').get('notebookId'))

# COMMAND ----------

## Simulate job run

job_session = {'rootRunId': {'id': 23143121}, 'currentRunId': {'id': 23143121}, 'jobGroup': '1932365121575318348_6211542408476119426_job-718537778382911-run-23143121-action-3496336520410385', 'tags': {'jobName': 'Multi Statement Transactions', 'opId': 'ServerBackend-7909fd45899d711', 'jobOwnerId': '20904982561468', 'opTarget': 'com.databricks.backend.common.rpc.InternalDriverBackendMessages$StartRepl', 'taskDependencies': '[]', 'eventName': 'runExecution', 'serverBackendName': 'com.databricks.backend.daemon.driver.DriverCorral', 'notebookId': '3624174420665873', 'projectName': 'driver', 'jobRunOriginalAttempt': '23143121', 'jobMiscMessage': 'In run', 'jobTriggerTime': '1658766611780', 'jobRunAttempt': '23143121', 'httpTarget': '/api/2.0/workspace/get-notebook-snapshot', 'buildHash': 'a2e5769182f120d638a865bc99430452da7670de', 'host': '10.0.7.51', 'notebookLanguage': None, 'sparkVersion': '', 'jobTriggerSource': 'DbScheduler', 'multitaskParentRunId': '23142588', 'hostName': '0614-181343-gmgfwres-10-0-7-51', 'httpMethod': 'GET', 'jettyRpcJettyVersion': '9', 'sourceIpAddress': '52.27.216.188', 'orgId': '1444828305810485', 'userAgent': 'Apache-HttpClient/4.5.9 (Java/11.0.7)', 'jobType': 'NORMAL', 'clusterId': '0614-181343-gmgfwres', 'jobTimeoutSec': '0', 'serverEventId': 'CgwIlIr7lgYQoJuDsQIQARidAyIkMDJlNWNjZmQtZmNhZi00MGI1LWEyZjgtY2M4ZmRjODIwZmU5', 'maxConcurrentRuns': '1', 'rootOpId': 'ServiceMain-d0a9f3e4c65e0002', 'idInJob': '23143121', 'jobClusterType': 'existing', 'executorName': 'ActiveRunMonitor-job-run-pool', 'sessionId': 'ephemeral-80f7a6cf-8496-41b9-880c-5e55fdbd980c', 'jobId': '718537778382911', 'notebookFullPath': None, 'requestId': '095084df-7d24-4d6d-90a0-73bd97d0c41f', 'jobTerminalState': 'Running', 'taskKey': 'Multi_Statement_Transactions', 'userId': '20904982561468', 'jobTriggerId': '0', 'opType': 'ServerBackend', 'jobTriggerType': 'manual', 'clusterScalingType': None, 'jobTaskType': 'notebook', 'sourcePortNumber': '0', 'isGitRun': 'false', 'runId': '23143121', 'user': 'cody.davis@databricks.com', 'parentOpId': 'RPCClient-d0a9f3e4c65e5fc9', 'jettyRpcType': 'InternalDriverBackendMessages$DriverBackendRequest'}, 'extraContext': {'mlflowGitRelativePath': 'Multi Statement Transactions', 'non_uc_api_token': '', 'mlflowGitStatus': 'unknown', 'mlflowGitReference': 'main', 'mlflowGitUrl': 'https://github.com/CodyAustinDavis/edw-best-practices.git', 'notebook_path': '/Repos/cody.davis@databricks.com/edw-best-practices/Multi Statement Transactions', 'mlflowGitCommit': '5cbc78636fa9b8a598b08e48d111a6941324f13d', 'mlflowGitReferenceType': 'branch', 'api_url': 'https://oregon.cloud.databricks.com', 'aclPathOfAclRoot': '/jobs/718537778382911', 'mlflowGitProvider': 'gitHub', 'api_token': '[REDACTED]'}, 'credentialKeys': ['adls_aad_token', 'adls_gen2_aad_token', 'synapse_aad_token']}



# COMMAND ----------

print(raw_session.get('tags').get('jobName'))
print(raw_session.get('tags').get('jobId'))
print(raw_session.get('tags').get('runId'))
print(raw_session.get('tags').get('notebookId'))

# COMMAND ----------

print(job_session.get('tags').get('jobName'))
print(job_session.get('tags').get('jobId'))
print(job_session.get('tags').get('runId'))
print(job_session.get('tags').get('notebookId'))
print(datetime.now())

# COMMAND ----------

## Steps

"""
For all tables in the transaction
-- 0. Current state collection of all tables in SQL statement -- snapshot
-- 1. If transaction fails at any step
-- 2. Get all tables in the transaction (TBD)
-- 3. For each table - RESTORE <table> VERSION AS OF <most recent version since before the LAST serializiable operation BEFORE transaction started
-- 4. Return clean tables and reset 

-- TO DO: Figure out how to do this with checkpoints -- oooo
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY codydemos.bronze_allsensors;

# COMMAND ----------

sqlString = """
--Statement 1 -- the load
COPY INTO iot_dashboard.bronze_sensors
FROM (SELECT 
      id::bigint AS Id,
      device_id::integer AS device_id,
      user_id::integer AS user_id,
      calories_burnt::decimal(10,2) AS calories_burnt, 
      miles_walked::decimal(10,2) AS miles_walked, 
      num_steps::decimal(10,2) AS num_steps, 
      timestamp::timestamp AS timestamp,
      value AS value -- This is a JSON object
FROM "/databricks-datasets/iot-stream/data-device/")
FILEFORMAT = json
COPY_OPTIONS('force'='true') -- 'false' -- process incrementally
--option to be incremental or always load all files
; 

-- Statement 2
MERGE INTO iot_dashboard.silver_sensors AS target
USING (SELECT Id::integer,
              device_id::integer,
              user_id::integer,
              calories_burnt::decimal,
              miles_walked::decimal,
              num_steps::decimal,
              timestamp::timestamp,
              value::string
              FROM iot_dashboard.bronze_sensors) AS source
ON source.Id = target.Id
AND source.user_id = target.user_id
AND source.device_id = target.device_id
WHEN MATCHED THEN UPDATE SET 
  target.calories_burnt = source.calories_burnt,
  target.miles_walked = source.miles_walked,
  target.num_steps = source.num_steps,
  target.timestamp = source.timestamp
WHEN NOT MATCHED THEN INSERT *;

-- This calculate table stats for all columns to ensure the optimizer can build the best plan
-- Statement 3
ANALYZE TABLE iot_dashboard.silver_sensors COMPUTE STATISTICS FOR ALL COLUMNS;

-- Statement 4
-- Truncate bronze batch once successfully loaded
TRUNCATE TABLE iot_dashboard.bronze_sensors;
"""

## SQL Parser does not support customer COPY INTO, MERGE, ANALYZE TABLE, or OPTIMIZE, TRUNCATE statment, replace COPY with INSERT just to get table names
parsedString = (sqlString
                 .replace("COPY", "INSERT")
                 .replace("MERGE", "INSERT")
                 .replace("TRUNCATE TABLE", "SELECT 1 FROM")
                 .replace("ANALYZE TABLE", "SELECT 1 FROM")
                 .replace("COMPUTE STATISTICS FOR ALL COLUMNS", "") ## Add other variations if you want, deal with OPTIMIZE / Z ORDER later
                 .split(";") ## Get all tables in 
                )

# COMMAND ----------

# DBTITLE 1,Get Clean Distinct list of Tables
import re


allTablesInTransaction = [Parser(parsed_String[i]).tables for i,v in enumerate(parsedString)]
flattenedTablesInTransaction = [i for x in allTablesInTransaction for i in x]
## Remove raw file read statements in FROM
p = re.compile('[^/|^dbfs:/|^s3a://|^s3://]')

## Get Final Clean Tables
cleanedTablesInTransaction = list(set([ s for s in flattenedTablesInTransaction if p.match(s) ]))

print(cleanedTablesInTransaction)

# COMMAND ----------

from pyspark.sql.functions import *
transactionStartTime = datetime.now()

print(f"Transaction Start Time: {datetime.now()}")

for i in cleanedTablesInTransaction:
  
  ## During the transaction -- other versions can be added, so you want most recent version IF fails before this specific write attempt of this job
  latestVersion = spark.sql(f"""DESCRIBE HISTORY {i}""").agg(max(col("version"))).collect()[0][0]
  
  print(f"Starting Version: {i} at version {latestVersion}")
