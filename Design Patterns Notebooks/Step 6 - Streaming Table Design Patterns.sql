-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## This notebook will show a basic ETL design pattern on DBSQL with Streaming Tables
-- MAGIC
-- MAGIC 1. Using read_files/read_kafka with Streaming tables for ingestion
-- MAGIC 2. How to orhcestrate -- refresh schedule or external orchestrator
-- MAGIC 3. Reading from streaming tables incrementally
-- MAGIC 4. Upserting data into target tables from Streaming Tables
-- MAGIC 5. Materialized Views as Gold Layer
-- MAGIC 6. Limitations
-- MAGIC
-- MAGIC ### FAQ: 
-- MAGIC
-- MAGIC 1. If I REFRESH a streaming table in a Multi task job, and another task depends on it, will that task wait until the streaming table is complete? 
-- MAGIC
-- MAGIC 2. How can I read incrementally from a streaming table? Can I stream from it in python? Can I do batch style and delete data from the table once it is processed downstream without breaking things?
-- MAGIC
-- MAGIC 3. What if I want to UPSERT/ MERGE data into a target table? Are materialized views my only option?
-- MAGIC
-- MAGIC 4. Can I use streaming tables for ingesting into delta, then manage batches myself downstream with timestamp watermarking? This is a popular EDW loading pattern and I dont want to move to all DLT based development. All I want is streaming tables to create batches for me, then I want to do anything I want to, similar to staging tables in batch loading. This removes all restrictions I have downstream. How do I do this? 
-- MAGIC
-- MAGIC
-- MAGIC 5. Can I optimize / sort my streaming tables? I would need to do this to do manual watermarking to filter on ingest timestamp / checksum. 
-- MAGIC
-- MAGIC 6. If I cant, and I dont want to use materialized views or full loads for ALL downstream tables, then I would just use DBT + COPY INTO drops.
-- MAGIC
-- MAGIC 7. Can I accomplish this with COPY INTO if I need a classical loading pattern? Would be great if I could do the same thing but with a Kafka data source as well. 
-- MAGIC
-- MAGIC -----
-- MAGIC
-- MAGIC Streaming tables are refreshed and governed by DLT on the backend. With streaming tables, DLT is only the engine but orchestration is handled by the user either via schedules or external orchestrator Refreshes. 
-- MAGIC Streaming tables and Materialized Views run on Serverless pipelines under the DLT Serverless SKU: $0.4 /DBU Premium, $0.5 /DBU Enterprise. 
-- MAGIC This is CHEAPER than just the Serverless SQL warehouses - so it is a huge benefit to have automatically managed streaming tables and materialized views from a price/performance perspective
-- MAGIC Snowflake charges MORE or SAME for various workload types (such as the 1.5 multiplier just for tasks), this makes ETL on Databricks HUGELY price/performant

-- COMMAND ----------

-- DBTITLE 1,ONLY UNITY CATALOG TABLES ARE SUPPORTED

CREATE DATABASE IF NOT EXISTS main.iot_dashboard;
USE main.iot_dashboard;

-- COMMAND ----------

-- DBTITLE 1,Reading Data From Raw Files / Stream in Streaming Tables for BRONZE layer
-- MAGIC %sql
-- MAGIC DROP TABLE IF EXISTS main.iot_dashboard.streaming_tables_raw_data;
-- MAGIC
-- MAGIC -- Does not support CREATE OR REPLACE
-- MAGIC CREATE OR REFRESH STREAMING TABLE main.iot_dashboard.streaming_tables_raw_data
-- MAGIC   AS SELECT 
-- MAGIC       id::bigint AS Id,
-- MAGIC       device_id::integer AS device_id,
-- MAGIC       user_id::integer AS user_id,
-- MAGIC       calories_burnt::decimal(10,2) AS calories_burnt, 
-- MAGIC       miles_walked::decimal(10,2) AS miles_walked, 
-- MAGIC       num_steps::decimal(10,2) AS num_steps, 
-- MAGIC       timestamp::timestamp AS timestamp,
-- MAGIC       value  AS value -- This is a JSON object
-- MAGIC   FROM STREAM read_files('dbfs:/databricks-datasets/iot-stream/data-device/');
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Select from Streaming Table
SELECT * FROM main.iot_dashboard.streaming_tables_raw_data

-- COMMAND ----------

SELECT COUNT(0) AS DataSourceRowCount FROM main.iot_dashboard.streaming_tables_raw_data

-- COMMAND ----------

-- DBTITLE 1,Adding a Refresh schedule
-- Adds a schedule to refresh the streaming table once a day
-- at midnight in Los Angeles
ALTER STREAMING TABLE main.iot_dashboard.streaming_tables_raw_data
ADD SCHEDULE CRON '0 0 0 * * ? *' 
AT TIME ZONE 'America/Los_Angeles' --Timezone optional -- defaults to timezone session is located in
;

-- COMMAND ----------

-- Remove the schedule
ALTER STREAMING TABLE main.iot_dashboard.streaming_tables_raw_data
DROP SCHEDULE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC -----

-- COMMAND ----------

-- DBTITLE 1,Reading from a streaming table: Option 2 - Incremental reads with Streaming --> Batch Mode
CREATE OR REFRESH STREAMING TABLE main.iot_dashboard.streaming_silver_staging
AS 
SELECT *,
now() AS processed_watermark
FROM STREAM main.iot_dashboard.streaming_tables_raw_data
CLUSTER BY (processed_watermark);

-- COMMAND ----------

REFRESH STREAMING TABLE main.iot_dashboard.streaming_silver_staging FULL

-- COMMAND ----------

-- DBTITLE 1,Optimize Streaming Files to Prep - Only if manually orchestrating insert - otherwise use STREAM tables or MVs
OPTIMIZE main.iot_dashboard.streaming_silver_staging ZORDER BY (processed_watermark, `timestamp`);

-- COMMAND ----------

-- DBTITLE 1,Silver Tables - Create DDL for 
CREATE OR REPLACE TABLE iot_dashboard.streaming_silver_sensors
(
Id BIGINT GENERATED BY DEFAULT AS IDENTITY,
device_id INT,
user_id INT,
calories_burnt DECIMAL(10,2), 
miles_walked DECIMAL(10,2), 
num_steps DECIMAL(10,2), 
timestamp TIMESTAMP,
value STRING,
processed_time TIMESTAMP
)
USING DELTA 
PARTITIONED BY (user_id);

-- COMMAND ----------

SELECT * FROM main.iot_dashboard.streaming_silver_staging;

-- COMMAND ----------

MERGE INTO iot_dashboard.streaming_silver_sensors AS target
USING (
WITH de_dup (
SELECT Id::integer,
              device_id::integer,
              user_id::integer,
              calories_burnt::decimal,
              miles_walked::decimal,
              num_steps::decimal,
              timestamp::timestamp,
              value::string,
              processed_watermark,
              ROW_NUMBER() OVER(PARTITION BY device_id, user_id, timestamp ORDER BY timestamp DESC) AS DupRank
              FROM main.iot_dashboard.streaming_silver_staging
              WHERE processed_watermark > (SELECT MAX(processed_time)::timestamp FROM iot_dashboard.streaming_silver_sensors)
              )
              
SELECT Id, device_id, user_id, calories_burnt, miles_walked, num_steps, timestamp, value, processed_watermark AS processed_time
FROM de_dup
WHERE DupRank = 1
) AS source
ON source.Id = target.Id
AND source.user_id = target.user_id
AND source.device_id = target.device_id
WHEN MATCHED THEN UPDATE SET 
  target.calories_burnt = source.calories_burnt,
  target.miles_walked = source.miles_walked,
  target.num_steps = source.num_steps,
  target.timestamp = source.timestamp
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

SELECT Id::integer,
              device_id::integer,
              user_id::integer,
              calories_burnt::decimal,
              miles_walked::decimal,
              num_steps::decimal,
              timestamp::timestamp,
              value::string,
              processed_watermark,
              ROW_NUMBER() OVER(PARTITION BY device_id, user_id, timestamp ORDER BY timestamp DESC) AS DupRank
              FROM main.iot_dashboard.streaming_silver_staging
              WHERE processed_watermark > (SELECT COALESCE(MAX(processed_time)::timestamp, '1900-01-01T00:00:00'::timestamp) FROM iot_dashboard.streaming_silver_sensors)

-- COMMAND ----------

SELECT * FROM main.iot_dashboard.streaming_silver_staging
