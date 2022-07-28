# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Table Optimization Methods

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Delta Tables
# MAGIC 
# MAGIC ### File Sizes
# MAGIC 
# MAGIC #### COMPACTION
# MAGIC 
# MAGIC ##### ZORDER 
# MAGIC 
# MAGIC ###### Bloom Filter

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS iot_dashboard.bronze_sensors_test1;
# MAGIC CREATE OR REPLACE TABLE iot_dashboard.bronze_sensors_test1
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES("delta.targetFileSize"="2mb") --64-128 mb for tables with heavy updates or if used for BI
# MAGIC AS 
# MAGIC (SELECT * FROM iot_dashboard.silver_sensors LIMIT 10000) --Only load a subset for sample MERGE;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS iot_dashboard.silver_sensors_test1;
# MAGIC CREATE OR REPLACE TABLE iot_dashboard.silver_sensors_test1
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES("delta.targetFileSize"="2mb")
# MAGIC AS 
# MAGIC SELECT * fROM iot_dashboard.silver_sensors;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO iot_dashboard.silver_sensors_test1 AS target
# MAGIC USING (SELECT Id::integer,
# MAGIC               device_id::integer,
# MAGIC               user_id::integer,
# MAGIC               calories_burnt::decimal,
# MAGIC               miles_walked::decimal,
# MAGIC               num_steps::decimal,
# MAGIC               timestamp::timestamp,
# MAGIC               value::string
# MAGIC               FROM iot_dashboard.bronze_sensors_test1) AS source
# MAGIC ON source.Id = target.Id
# MAGIC AND source.user_id = target.user_id
# MAGIC AND source.device_id = target.device_id
# MAGIC AND target.timestamp > now() - INTERVAL 2 hours
# MAGIC WHEN MATCHED THEN UPDATE SET 
# MAGIC   target.calories_burnt = source.calories_burnt,
# MAGIC   target.miles_walked = source.miles_walked,
# MAGIC   target.num_steps = source.num_steps,
# MAGIC   target.timestamp = source.timestamp
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC 
# MAGIC -- Without optimizing tables 8.82 seconds
# MAGIC -- After optimizing by merge columns 19 seconds

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- You want to optimize by high cardinality columns like ids, timestamps, strings
# MAGIC -- ON MERGE COLUMNS, then timeseries columns, then commonly used columns in queries
# MAGIC 
# MAGIC --This operation is incremental
# MAGIC --OPTIMIZE iot_dashboard.bronze_sensors_test1 ZORDER BY (Id, user_id, device_id);
# MAGIC OPTIMIZE iot_dashboard.silver_sensors_test1 ZORDER BY (user_id, device_id, Id);

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## What about queries on this table?
# MAGIC 
# MAGIC 1. ZORDER by commonly joined columns
# MAGIC 2. Partition by larger chunks only if needed
# MAGIC 3. Keep important columns in front of tables
# MAGIC 4. For highly selective queries, use bloom indexes

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise 1: Change optimization strategies for single point filters

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE iot_dashboard.silver_sensors_test1 ZORDER BY (user_id);
# MAGIC 
# MAGIC -- by user_id, timestamp -- 8 files pruned
# MAGIC -- by just user id selecting on user_id -- 34 files pruned (1 read) all but one
# MAGIC -- by just timestamp -- no files pruned when selecting on user_id

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW iot_dashboard.hourly_summary_statistics
# MAGIC AS
# MAGIC SELECT user_id,
# MAGIC date_trunc('hour', timestamp) AS HourBucket,
# MAGIC AVG(num_steps) AS AvgNumStepsAcrossDevices,
# MAGIC AVG(calories_burnt) AS AvgCaloriesBurnedAcrossDevices,
# MAGIC AVG(miles_walked) AS AvgMilesWalkedAcrossDevices
# MAGIC FROM iot_dashboard.silver_sensors_test1
# MAGIC GROUP BY user_id,date_trunc('hour', timestamp) -- wrapping a function around a column
# MAGIC ORDER BY HourBucket

# COMMAND ----------

# DBTITLE 1,Exercise 1: Tuning for single column queries
# MAGIC %sql
# MAGIC -- After optimize look at user_id files prune
# MAGIC -- by user_id, timestamp -- 8 files pruned
# MAGIC -- by just user id selecting on user_id -- 34 files pruned (1 read) all but one
# MAGIC -- by just timestamp -- no files pruned when selecting on user_is
# MAGIC 
# MAGIC SELECT * FROM iot_dashboard.hourly_summary_statistics WHERe user_id = 1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exercise 2: Multi-dimensional filters and optimzation

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC SELECT MIN(HourBucket), MAX(HourBucket)
# MAGIC FROM iot_dashboard.hourly_summary_statistics 

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE iot_dashboard.silver_sensors_test1 ZORDER BY (user_id, timestamp);
# MAGIC 
# MAGIC -- by user_id, timestamp -- 2 files pruned, 29 scanned
# MAGIC -- by timestamp, user_id --  does order matter? 2 files pruned, 29 scanned, - not really
# MAGIC -- How to make this more selective? -- Hour bucket is abstracting the filter pushdown, lets try just the raw table

# COMMAND ----------

# DBTITLE 1,Exercise 2: Optimizing Multi-dimensional queries
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM iot_dashboard.hourly_summary_statistics 
# MAGIC WHERE user_id = 1
# MAGIC AND HourBucket BETWEEN "2018-07-22T00:00:00.000+0000" AND "2018-07-22T01:00:00.000+0000"

# COMMAND ----------

# DBTITLE 1,Lesson learned -- let Delta do the filtering first, then group and aggregate -- subqueries are actually better
# MAGIC %sql
# MAGIC 
# MAGIC --28 pruned, 3 files read
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM iot_dashboard.silver_sensors_test1
# MAGIC WHERE user_id = 1
# MAGIC AND timestamp BETWEEN "2018-07-22T00:00:00.000+0000"::timestamp AND "2018-07-22T01:00:00.000+0000"::timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW iot_dashboard.test_filter_pushdown
# MAGIC AS 
# MAGIC WITH raw_pushdown AS
# MAGIC (
# MAGIC   SELECT * 
# MAGIC   FROM iot_dashboard.silver_sensors_test1
# MAGIC   WHERE user_id = 1
# MAGIC   AND timestamp BETWEEN "2018-07-22T00:00:00.000+0000"::timestamp AND "2018-07-22T01:00:00.000+0000"::timestamp
# MAGIC )
# MAGIC SELECT user_id,
# MAGIC date_trunc('hour', timestamp) AS HourBucket,
# MAGIC AVG(num_steps) AS AvgNumStepsAcrossDevices,
# MAGIC AVG(calories_burnt) AS AvgCaloriesBurnedAcrossDevices,
# MAGIC AVG(miles_walked) AS AvgMilesWalkedAcrossDevices
# MAGIC FROM raw_pushdown
# MAGIC GROUP BY user_id,date_trunc('hour', timestamp)
# MAGIC ORDER BY HourBucket

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM iot_dashboard.test_filter_pushdown

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW iot_dashboard.smoothed_hourly_statistics
# MAGIC AS 
# MAGIC SELECT *,
# MAGIC -- Number of Steps
# MAGIC (avg(`AvgNumStepsAcrossDevices`) OVER (
# MAGIC         ORDER BY `HourBucket`
# MAGIC         ROWS BETWEEN
# MAGIC           4 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       )) ::float AS SmoothedNumSteps4HourMA, -- 4 hour moving average
# MAGIC       
# MAGIC (avg(`AvgNumStepsAcrossDevices`) OVER (
# MAGIC         ORDER BY `HourBucket`
# MAGIC         ROWS BETWEEN
# MAGIC           24 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ))::float AS SmoothedNumSteps12HourMA --24 hour moving average
# MAGIC ,
# MAGIC -- Calories Burned
# MAGIC (avg(`AvgCaloriesBurnedAcrossDevices`) OVER (
# MAGIC         ORDER BY `HourBucket`
# MAGIC         ROWS BETWEEN
# MAGIC           4 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ))::float AS SmoothedCalsBurned4HourMA, -- 4 hour moving average
# MAGIC       
# MAGIC (avg(`AvgCaloriesBurnedAcrossDevices`) OVER (
# MAGIC         ORDER BY `HourBucket`
# MAGIC         ROWS BETWEEN
# MAGIC           24 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ))::float AS SmoothedCalsBurned12HourMA --24 hour moving average,
# MAGIC ,
# MAGIC -- Miles Walked
# MAGIC (avg(`AvgMilesWalkedAcrossDevices`) OVER (
# MAGIC         ORDER BY `HourBucket`
# MAGIC         ROWS BETWEEN
# MAGIC           4 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ))::float AS SmoothedMilesWalked4HourMA, -- 4 hour moving average
# MAGIC       
# MAGIC (avg(`AvgMilesWalkedAcrossDevices`) OVER (
# MAGIC         ORDER BY `HourBucket`
# MAGIC         ROWS BETWEEN
# MAGIC           24 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ))::float AS SmoothedMilesWalked12HourMA --24 hour moving average
# MAGIC FROM iot_dashboard.hourly_summary_statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECt * FROM iot_dashboard.smoothed_hourly_statistics WHERE user_id = 1
