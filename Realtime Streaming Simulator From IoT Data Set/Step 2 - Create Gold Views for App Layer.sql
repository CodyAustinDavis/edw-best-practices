-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ## Dashboard Recommendations
-- MAGIC 
-- MAGIC 1. Pushdown timestamp filters as much as possible
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

-- DBTITLE 1,Example: Generate Query from Dashboard Engine
--This ensure the data prunes all data older than needed depending on the use case

SELECT * FROM real_time_iot_dashboard.gold_sensors
WHERE timestamp >= (current_timestamp() - INTERVAL '1 hour')
