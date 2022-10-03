-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # This notebook generates a full data pipeline from databricks dataset - iot-stream
-- MAGIC 
-- MAGIC ## This creates 2 tables: 
-- MAGIC 
-- MAGIC <b> Database: </b> iot_dashboard
-- MAGIC 
-- MAGIC <b> Tables: </b> silver_sensors, silver_users 
-- MAGIC 
-- MAGIC <b> Params: </b> StartOver (Yes/No) - allows user to truncate and reload pipeline

-- COMMAND ----------

-- DBTITLE 1,Medallion Architecture
-- MAGIC %md
-- MAGIC 
-- MAGIC <img src="https://databricks.com/wp-content/uploads/2022/03/delta-lake-medallion-architecture-2.jpeg" >

-- COMMAND ----------

DROP DATABASE IF EXISTS iot_dashboard CASCADE;
CREATE DATABASE IF NOT EXISTS iot_dashboard;
USE iot_dashboard;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # DDL Documentation: 
-- MAGIC 
-- MAGIC https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-alter-table.html

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS iot_dashboard.bronze_sensors
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
TBLPROPERTIES("delta.targetFileSize"="128mb")
-- Other helpful properties
-- delta.dataSkippingNumIndexedCols -- decides how many columns are automatically tracked with statistics kepts (defaults to first 32)
-- LOCATION "s3://bucket-name/data_lakehouse/tables/data/bronze/bronze_senors/"
;

-- COMMAND ----------

-- DBTITLE 1,Look at Table Details
-- MAGIC %sql
-- MAGIC 
-- MAGIC DESCRIBE TABLE EXTENDED iot_dashboard.bronze_sensors

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS iot_dashboard.bronze_users
(
userid BIGINT GENERATED BY DEFAULT AS IDENTITY,
gender STRING,
age INT,
height DECIMAL(10,2), 
weight DECIMAL(10,2),
smoker STRING,
familyhistory STRING,
cholestlevs STRING,
bp STRING,
risk DECIMAL(10,2),
update_timestamp TIMESTAMP
)
USING DELTA 
TBLPROPERTIES("delta.targetFileSize"="128mb") 
--LOCATION s3://<path>/
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Exhaustive list of all COPY INTO Options
-- MAGIC https://docs.databricks.com/sql/language-manual/delta-copy-into.html#format-options-1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## New FEATURES IN DBR 11!
-- MAGIC 
-- MAGIC 1. COPY INTO GENERIC TABLE
-- MAGIC 2. DROP COLUMN STATEMENT
-- MAGIC 3. Select all except: SELECT * EXCEPT (col1,...) FROM table
-- MAGIC 
-- MAGIC https://docs.databricks.com/release-notes/runtime/11.0.html

-- COMMAND ----------


--With DBR 11, we dont need to specify DDL first
--CREATE TABLE IF NOT EXISTS iot_dashboard.bronze_sensors

--COPY INTO iot_dashboard.bronze_sensors
--FROM (SELECT 
--      id::bigint AS Id,
--      device_id::integer AS device_id,
--      user_id::integer AS user_id,
--      calories_burnt::decimal(10,2) AS calories_burnt, 
--      miles_walked::decimal(10,2) AS miles_walked, 
--      num_steps::decimal(10,2) AS num_steps, 
--     timestamp::timestamp AS timestamp,
--      value AS value -- This is a JSON object
--FROM "/databricks-datasets/iot-stream/data-device/")
--FILEFORMAT = json
--COPY_OPTIONS('force'='true') -- 'false' -- process incrementally
--option to be incremental or always load all files
 


-- COMMAND ----------

-- DBTITLE 1,Incrementally Ingest Source Data from Raw Files
COPY INTO iot_dashboard.bronze_sensors
FROM (SELECT 
      id::bigint AS Id,
      device_id::integer AS device_id,
      user_id::integer AS user_id,
      calories_burnt::decimal(10,2) AS calories_burnt, 
      miles_walked::decimal(10,2) AS miles_walked, 
      num_steps::decimal(10,2) AS num_steps, 
      timestamp::timestamp AS timestamp,
      value  AS value -- This is a JSON object
FROM "/databricks-datasets/iot-stream/data-device/")
FILEFORMAT = json
COPY_OPTIONS('force'='false') --'true' always loads all data it sees. option to be incremental or always load all files


--Other Helpful copy options: 
/*
PATTERN('[A-Za-z].csv')
FORMAT_OPTIONS ('ignoreCorruptFiles' = 'true') -- skips bad files for more robust incremental loads
COPY_OPTIONS ('mergeSchema' = 'true')
'ignoreChanges' = 'true'
*/;

-- COMMAND ----------

-- DBTITLE 1,Create Silver Table for upserting updates
CREATE TABLE IF NOT EXISTS iot_dashboard.silver_sensors
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
TBLPROPERTIES("delta.targetFileSize"="128mb") -- if update heavy, file sizes are great between 64-128 mbs. The more update heavy, the smaller the files (32-256mb)
--LOCATION s3://<path>/ -- Always specify location for production tables so you control where it lives in S3/ADLS/GCS
-- Not specifying location parth will put table in DBFS, a managed bucket that cannot be accessed by apps outside of databricks
;

-- COMMAND ----------

-- DBTITLE 1,Perform Upserts - Device Data
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
ANALYZE TABLE iot_dashboard.silver_sensors COMPUTE STATISTICS FOR ALL COLUMNS;
OPTIMIZE iot_dashboard.silver_sensors ZORDER BY (timestamp);

-- Truncate bronze batch once successfully loaded
TRUNCATE TABLE iot_dashboard.bronze_sensors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exhaustive list of optimizations on Delta Tables
-- MAGIC https://docs.databricks.com/delta/optimizations/file-mgmt.html#set-a-target-size

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Levels of optimization on Databricks
-- MAGIC 
-- MAGIC ## Partitions - Do not over partition - usually ZORDERING covers what you need - even in big tables
-- MAGIC ### File Sizes - smaller for BI heavy and update heavy tables 64mb to 128mb
-- MAGIC #### Order of files -- ZORDER(col,col) -- ZORDER on most used filtering/join columns, in order of cardinality like a funnel
-- MAGIC ##### Indexes -- For highly selective queries - need to create index first then fill with data "needle in a haystack"

-- COMMAND ----------

ALTER TABLE iot_dashboard.silver_sensors SET TBLPROPERTIES ('delta.targetFileSize'='64mb');

-- COMMAND ----------

-- DBTITLE 1,Table Optimizations
-- You want to optimize by high cardinality columns like ids, timestamps, strings
OPTIMIZE iot_dashboard.silver_sensors ZORDER BY (user_id, device_id, timestamp);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Details on Bloom Indexs Here:
-- MAGIC https://docs.databricks.com/delta/optimizations/bloom-filters.html

-- COMMAND ----------

--Bloom filters need to exist first, so if you add an index later you need to reprocess the files (an optimize, etc.)

--Ideally a column that is highly selective but not used in z-order (text, other timestamps, etc.)
CREATE BLOOMFILTER INDEX
ON TABLE iot_dashboard.silver_sensors
FOR COLUMNS(device_id OPTIONS (fpp=0.1, numItems=50000000))

-- COMMAND ----------

-- DBTITLE 1,Select Semi Structured/Unstructred Data with JSON dot notation
SELECT 
*,
value:user_id::integer AS parsed_user,
value:time_stamp::timestamp AS parsed_time -- Pro tip: You can do the same thing if reading in json via the text reader. Makes for highly flexible data ingestion
FROM iot_dashboard.silver_sensors;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## Ingest User Data As Well

-- COMMAND ----------

-- DBTITLE 1,Incrementally Ingest Raw User Data
COPY INTO iot_dashboard.bronze_users
FROM (SELECT 
      userid::bigint AS userid,
      gender AS gender,
      age::integer AS age,
      height::decimal(10,2) AS height, 
      weight::decimal(10,2) AS weight,
      smoker AS smoker,
      familyhistory AS familyhistory,
      cholestlevs AS cholestlevs,
      bp AS bp,
      risk::decimal(10,2) AS risk,
      current_timestamp() AS update_timestamp
FROM "/databricks-datasets/iot-stream/data-user/")
FILEFORMAT = CSV
FORMAT_OPTIONS('header'='true')
COPY_OPTIONS('force'='true') --option to be incremental or always load all files
;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS iot_dashboard.silver_users
(
userid BIGINT GENERATED BY DEFAULT AS IDENTITY,
gender STRING,
age INT,
height DECIMAL(10,2), 
weight DECIMAL(10,2),
smoker STRING,
familyhistory STRING,
cholestlevs STRING,
bp STRING,
risk DECIMAL(10,2),
update_timestamp TIMESTAMP
)
USING DELTA 
TBLPROPERTIES("delta.targetFileSize"="128mb")
--LOCATION s3://<path>/ -- Always specify path for production tables. 
;

-- COMMAND ----------

MERGE INTO iot_dashboard.silver_users AS target
USING (SELECT 
      userid::int,
      gender::string,
      age::int,
      height::decimal, 
      weight::decimal,
      smoker,
      familyhistory,
      cholestlevs,
      bp,
      risk,
      update_timestamp
      FROM iot_dashboard.bronze_users) AS source
ON source.userid = target.userid
WHEN MATCHED THEN UPDATE SET 
  target.gender = source.gender,
      target.age = source.age,
      target.height = source.height, 
      target.weight = source.weight,
      target.smoker = source.smoker,
      target.familyhistory = source.familyhistory,
      target.cholestlevs = source.cholestlevs,
      target.bp = source.bp,
      target.risk = source.risk,
      target.update_timestamp = source.update_timestamp
WHEN NOT MATCHED THEN INSERT *;

--Truncate bronze batch once successfully loaded
TRUNCATE TABLE iot_dashboard.bronze_users;

-- COMMAND ----------

OPTIMIZE iot_dashboard.silver_users ZORDER BY (userid);

-- COMMAND ----------

SELECT * FROM iot_dashboard.silver_users;

-- COMMAND ----------

SELECT * FROM iot_dashboard.silver_sensors;
