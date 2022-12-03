-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ## Building Production Data Apps - Last Mile BI on Databricks and Dash
-- MAGIC 
-- MAGIC <b> Dash apps:  </b> https://dash.gallery/Portal/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <img src="https://miro.medium.com/max/1400/1*N2hJnle6RJ6HRRF4ISFBjw.gif">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Dashboard Recommendations
-- MAGIC 
-- MAGIC 1. Pushdown timestamp filters as much as possible (especially now that insert order is preserved)
-- MAGIC 2. Bring back as little data as necessary
-- MAGIC 3. Make the Lakehouse do all the work

-- COMMAND ----------

-- DBTITLE 1,Generate View with Heavy Logic
CREATE OR REPLACE VIEW real_time_iot_dashboard.gold_sensors
AS 
SELECT *,
-- Number of Steps
(avg(`num_steps`) OVER (
        ORDER BY timestamp
        ROWS BETWEEN
          30 PRECEDING AND
          CURRENT ROW
      )) ::float AS SmoothedNumSteps30SecondMA, -- 30 second moving average
     
(avg(`num_steps`) OVER (
        ORDER BY timestamp
        ROWS BETWEEN
          120 PRECEDING AND
          CURRENT ROW
      ))::float AS SmoothedMilesWalked120SecondMA --120 second moving average
FROM real_time_iot_dashboard.bronze_sensors
-- Photon likes things this way for some reason
WHERE timestamp::double >= ((SELECT MAX(timestamp)::double FROM real_time_iot_dashboard.bronze_sensors) - 60)
LIMIT 10000

-- COMMAND ----------

CREATE OR REPLACE VIEW real_time_iot_dashboard.gold_sensors_stateful
AS 
SELECT *,
-- Number of Steps
(avg(`num_steps`) OVER (
        ORDER BY EventStart
        ROWS BETWEEN
          30 PRECEDING AND
          CURRENT ROW
      )) ::float AS SmoothedNumSteps30SecondMA, -- 30 second moving average
     
(avg(`num_steps`) OVER (
        ORDER BY EventStart
        ROWS BETWEEN
          120 PRECEDING AND
          CURRENT ROW
      ))::float AS SmoothedNumSteps120SecondMA --120 second moving average
FROM real_time_iot_dashboard.silver_sensors_stateful
-- Photon likes things this way for some reason
-- Use sort order / zorder file level pruning, usually on timestamp and/or lookup keys (like device_id, user_id)
WHERE EventStart::double >= ((SELECT MAX(EventStart)::double FROM real_time_iot_dashboard.silver_sensors_stateful) - 60)
--Use partition pruning to ignore data as it ages
AND Date = ((SELECT MAX(Date) FROM real_time_iot_dashboard.silver_sensors_stateful))
LIMIT 10000

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC SELECT 
-- MAGIC COUNT(0) AS Records,
-- MAGIC MIN(timestamp) AS EarliestTimestamp,
-- MAGIC MAX(timestamp) AS MostRecentTimestamp,
-- MAGIC MIN(timestamp) -  MAX(timestamp) AS TotalSecondsOfData
-- MAGIC FROM real_time_iot_dashboard.bronze_sensors

-- COMMAND ----------

-- DBTITLE 1,Example of Dashboard Client Side Query
SELECT * 
FROM real_time_iot_dashboard.gold_sensors

-- COMMAND ----------

SELECT * FROM real_time_iot_dashboard.gold_sensors_stateful

-- COMMAND ----------

-- DBTITLE 1,Example: Generate Query from Dashboard Engine
--This ensure the data prunes all data older than needed depending on the use case

SELECT * FROM real_time_iot_dashboard.gold_sensors
WHERE timestamp >= (current_timestamp() - INTERVAL '1 hour')
AND user_id = 1;

-- COMMAND ----------

-- DBTITLE 1,Embed this into a Dash Callback to create automatically refreshing tables that trigger when the table updates
WITH log AS
(DESCRIBE HISTORY real_time_iot_dashboard.bronze_sensors
),
state AS (
SELECT
version,
timestamp,
operation
FROM log
WHERE (timestamp >= current_timestamp() - INTERVAL '24 hours')
AND operation IN ('MERGE', 'WRITE', 'DELETE', 'STREAMING UPDATE')
ORDER By version DESC
),
comparison AS (
SELECT DISTINCT
s1.version,
s1.timestamp,
s1.operation,
LAG(version) OVER (ORDER BY version) AS Previous_Version,
LAG(timestamp) OVER (ORDER BY timestamp) AS Previous_Timestamp
FROM state AS s1
ORDER BY version DESC)

SELECT
date_trunc('hour', timestamp) AS HourBlock,
AVG(timestamp::double - Previous_Timestamp::double) AS AvgUpdateFrequencyInSeconds
FROM comparison
GROUP BY date_trunc('hour', timestamp)
ORDER BY HourBlock
