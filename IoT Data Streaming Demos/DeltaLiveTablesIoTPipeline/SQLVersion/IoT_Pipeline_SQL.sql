-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # IoT DLT Pipeline - SQL 

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE Raw_IoT_AllSensors_SQL
COMMENT "This is the RAW All Sensors Table Partitioned By Sensor Measurement"
TBLPROPERTIES ("quality" = "bronze")
AS (
    SELECT
    *,
    input_file_name() AS inputFileName
    FROM cloud_files('dbfs:/FileStore/shared_uploads/cody.davis@databricks.com/IotDemo/', 'csv', map("schema","Skip STRING, SkipResult STRING, SkipTable STRING, WindowAverageStartDateTime STRING, WindowAverageStopDateTime STRING, MeasurementDateTime TIMESTAMP, SensorValue DECIMAL, SensorUnitDescription STRING, SensorMeasurement STRING, SensorLocation STRING, Id STRING",  "header", "true"))
)

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE Bronze_IoT_AllSensors_SQL
(
  CONSTRAINT mesurement_date_valid EXPECT (CAST(MeasurementDateTime AS TIMESTAMP) IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT sensor_value_valid EXPECT (SensorValue > 1) ON VIOLATION DROP ROW
)
COMMENT "This is the Bronze All Sensors Table Partitioned By Sensor Measurement"
TBLPROPERTIES ("quality" = "bronze")
 AS (
    SELECT
    WindowAverageStartDateTime AS MeasurementWindowStartTime,
    WindowAverageStopDateTime AS MeasurementWindowEndTime,
    MeasurementDateTime::timestamp AS MeasurementDateTime,
    SensorValue AS SensorValue,
    SensorUnitDescription AS SensorUnitDescription,
    SensorMeasurement AS SensorMeasurement,
    SensorLocation AS SensorLocation,
    uuidUdf() AS Id,
    input_file_name() AS inputFileName
    FROM stream(LIVE.Raw_IoT_AllSensors_SQL)
    )

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE silver_all_sensors;

APPLY CHANGES INTO LIVE.silver_all_sensors
FROM stream(LIVE.Bronze_IoT_AllSensors_SQL)
KEYS (Id)
  SEQUENCE BY MeasurementDateTime
    --[APPLY AS DELETE WHEN condition]
    --[APPLY AS TRUNCATE WHEN condition]
    --COLUMNS * EXCEPT (column)
