-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Engineering Pipeline </h1>
-- MAGIC 
-- MAGIC <h2> Batch ETL With Sensor Data </h2>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## SQL Best Practices:
-- MAGIC 
-- MAGIC 1. Cast Data Types
-- MAGIC 2. Key Generation - auto_id
-- MAGIC 3. Location Definition
-- MAGIC 4. Partition columns
-- MAGIC 5. Table Layout
-- MAGIC 6. Optimization

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <img src="https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Bronze.png" >

-- COMMAND ----------

CREATE WIDGET TEXT InputLocation DEFAULT  ""

-- COMMAND ----------

DROP DATABASE IF EXISTS demo_sql CASCADE;
CREATE DATABASE IF NOT EXISTS demo_sql;
USE demo_sql;

-- COMMAND ----------

-- DBTITLE 1,Create Staging Table
CREATE OR REPLACE TABLE bronze_staging_all_sensors
(
Id STRING,
MeasurementDateTime TIMESTAMP,
SensorValue DECIMAL(10,0),
SensorUnitDescription STRING,
SensorMeasurement STRING,
SensorLocation STRING,
InputFileName STRING
)
USING DELTA --default
PARTITIONED BY (SensorMeasurement);

-- COMMAND ----------

SELECT * FROM bronze_staging_all_sensors

-- COMMAND ----------

-- DBTITLE 1,Read Raw Data from the Lake - S3

--Read in any file, and even many files straight from SQL
-- Read file level meta-data
SELECT *, 
input_file_name(), --function to pull file name into columns and other meta data
_metadata.file_name,
_metadata.file_size,
_metadata.file_modification_time
FROM csv.`dbfs:/FileStore/shared_uploads/cody.davis@databricks.com/IotDemo/` --Can use Glob filters here like *

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## Exhaustive list of all COPY INTO Options
-- MAGIC https://docs.databricks.com/sql/language-manual/delta-copy-into.html#format-options-1

-- COMMAND ----------

-- DBTITLE 1,Incrementally Copy Data Into a Loading Table
--Run twice to show incrementalness
--Automatically incremental like readStream
COPY INTO bronze_staging_all_sensors --can be table name or file path
    FROM 
    (SELECT uuid() AS Id,
          _c5::timestamp AS MeasurementDateTime,
          _c6::decimal AS SensorValue,
          _c7::string AS SensorUnitDescription,
          _c8::string AS SensorMeasurement,
          _c9::string AS SensorLocation,
          input_file_name() AS InputFileName
    FROM '$InputLocation') --Getting Data Dynamically
    FILEFORMAT = CSV
    PATTERN = '*.csv' --Glob/regex pattern matching or list of file names
    FORMAT_OPTIONS('header' = 'false')
    COPY_OPTIONS('force'='false') -- 'false' to keep incrermental, true to always load all data it sees in the location, just like clearing a checkpoint so great to use for a staging table downstream

-- COMMAND ----------

SELECT * FROM bronze_staging_all_sensors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Next Step: Merge Upsert

-- COMMAND ----------

-- DBTITLE 1,Create Silver Target Table
CREATE OR REPLACE TABLE Silver_AllSensors_Simple 
(
MeasurementDateTime TIMESTAMP,
SensorValue DECIMAL(10,0),
SensorUnitDescription STRING,
SensorMeasurement STRING,
SensorLocation STRING,
Id STRING,
InputFileName STRING
)
USING DELTA
--LOCATION "/data/codydemos/silver_allsensors_simple_stream_2" --optionally decide where it lives
PARTITIONED BY (SensorLocation);

-- COMMAND ----------

-- DBTITLE 1,Perform a MERGE in Batch Mode
--We can do SCD 1, SCD 2, or anything else here
  MERGE INTO Silver_AllSensors_Simple AS target
  USING (SELECT * FROM bronze_staging_all_sensors WHERE MeasurementDateTime IS NOT NULL)
  AS source
  ON target.Id = source.Id
  WHEN NOT MATCHED
  THEN INSERT *;
  
  TRUNCATE TABLE bronze_staging_all_sensors;

-- COMMAND ----------

DESCRIBE HISTORY Silver_AllSensors_Simple;

-- COMMAND ----------


SELECT * FROM Silver_AllSensors_Simple

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
-- MAGIC ##### Idexes -- For highly selective queries - need to create index first then fill with data "needle in a haystack"

-- COMMAND ----------

-- DBTITLE 1,Optimize Tables

--Partitions -- covered above
--File Sizes
--Order of files
--Idexes
--Change all file sizes used on this cluster

--SET spark.databricks.delta.optimize.maxFileSize = 104857600;

--Just change file size of the table

ALTER TABLE Silver_AllSensors_Simple SET TBLPROPERTIES ('delta.targetFileSize'='64mb');


OPTIMIZE Silver_AllSensors_Simple ZORDER BY (MeasurementDateTime, sellerid, orderid);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Details on Bloom Indexs Here:
-- MAGIC https://docs.databricks.com/delta/optimizations/bloom-filters.html

-- COMMAND ----------

-- DBTITLE 1,Indexes - Bloom Filter Index

--Bloom filters need to exist first, so if you add an index later you need to reprocess the files (an optimize, etc.)

--Ideally a column that is highly selective but not used in z-order (text, other timestamps, etc.)
CREATE BLOOMFILTER INDEX
ON TABLE Silver_AllSensors_Simple
FOR COLUMNS(Id OPTIONS (fpp=0.1, numItems=50000000))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Gold Tables and Views!

-- COMMAND ----------


CREATE OR REPLACE TABLE gold_airtempanalysis 
USING DELTA 
AS (

  SELECT *,
  avg(`SensorValue`) OVER (
          ORDER BY `MeasurementDateTime`
          ROWS BETWEEN
            30 PRECEDING AND
            CURRENT ROW
        ) AS TempShortMovingAverage,

  avg(`SensorValue`) OVER (
          ORDER BY `MeasurementDateTime`
          ROWS BETWEEN
            365 PRECEDING AND
            CURRENT ROW
        ) AS TempLongMovingAverage
  FROM Silver_AllSensors_Simple
  WHERE SensorMeasurement = 'average_temperature'
);

-- COMMAND ----------


CREATE VIEW IF NOT EXISTS air_temp_ma_live
AS
SELECT *,
avg(`SensorValue`) OVER (
        ORDER BY `MeasurementDateTime`
        ROWS BETWEEN
          30 PRECEDING AND
          CURRENT ROW
      ) AS TempShortMovingAverage,
      
avg(`SensorValue`) OVER (
        ORDER BY `MeasurementDateTime`
        ROWS BETWEEN
          365 PRECEDING AND
          CURRENT ROW
      ) AS TempLongMovingAverage
  FROM Silver_AllSensors_Simple
  WHERE SensorMeasurement = 'average_temperature';


-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC SELECT * FROM air_temp_ma_live ORDER BY MeasurementDateTime
