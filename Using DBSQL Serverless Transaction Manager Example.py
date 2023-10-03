# Databricks notebook source
# MAGIC %pip install -r helperfunctions/requirements.txt

# COMMAND ----------

from helperfunctions.dbsqltransactions import DBSQLTransactionManager

# COMMAND ----------

# DBTITLE 1,Example Inputs For Client
token = None ## optional
host_name = None ## optional
warehouse_id = "475b94ddc7cd5211"

# COMMAND ----------

# DBTITLE 1,Example Multi Statement Transaction
sqlString = """
USE CATALOG hive_metastore;

CREATE SCHEMA IF NOT EXISTS iot_dashboard;

USE SCHEMA iot_dashboard;

-- Create Tables
CREATE OR REPLACE TABLE iot_dashboard.bronze_sensors
(
Id BIGINT GENERATED BY DEFAULT AS IDENTITY,
device_id INT,
user_id INT,
calories_burnt DECIMAL(10,2), 
miles_walked DECIMAL(10,2), 
num_steps DECIMAL(10,2), 
timestamp TIMESTAMP,
value STRING
)
USING DELTA
TBLPROPERTIES("delta.targetFileSize"="128mb");

CREATE OR REPLACE TABLE iot_dashboard.silver_sensors
(
Id BIGINT GENERATED BY DEFAULT AS IDENTITY,
device_id INT,
user_id INT,
calories_burnt DECIMAL(10,2), 
miles_walked DECIMAL(10,2), 
num_steps DECIMAL(10,2), 
timestamp TIMESTAMP,
value STRING
)
USING DELTA 
PARTITIONED BY (user_id)
TBLPROPERTIES("delta.targetFileSize"="128mb");

-- Statement 1 -- the load
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

OPTIMIZE iot_dashboard.silver_sensors ZORDER BY (timestamp);

-- This calculate table stats for all columns to ensure the optimizer can build the best plan
-- Statement 3

ANALYZE TABLE iot_dashboard.silver_sensors COMPUTE STATISTICS FOR ALL COLUMNS;

CREATE OR REPLACE TABLE hourly_summary_statistics
AS
SELECT user_id,
date_trunc('hour', timestamp) AS HourBucket,
AVG(num_steps)::float AS AvgNumStepsAcrossDevices,
AVG(calories_burnt)::float AS AvgCaloriesBurnedAcrossDevices,
AVG(miles_walked)::float AS AvgMilesWalkedAcrossDevices
FROM silver_sensors
GROUP BY user_id,date_trunc('hour', timestamp)
ORDER BY HourBucket;

-- Statement 4
-- Truncate bronze batch once successfully loaded
TRUNCATE TABLE bronze_sensors;
"""

# COMMAND ----------

serverless_client_t = DBSQLTransactionManager(warehouse_id = warehouse_id, mode="inferred_altered_tables") ## token=<optional>, host_name=<optional>verbose=True for print statements and other debugging messages

# COMMAND ----------

# DBTITLE 1,Submitting the Multi Statement Transaction to Serverless SQL Warehouse
"""
PARAMS: 
warehouse_id --> Required, the SQL warehouse to submit statements
mode -> selected_tables, inferred_altered_tables
token --> optional, will try to get one for the user
host_name --> optional, will try to infer same workspace url


execute_sql_transaction params: 
return_type --> "message", "last_results". "message" will return status of query chain. "last_result" will run all statements and return the last results of the final query in the chain

"""

result_df = serverless_client_t.execute_dbsql_transaction(sql_string = sqlString)
