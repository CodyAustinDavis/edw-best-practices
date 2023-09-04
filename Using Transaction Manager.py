# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## TO DO: 
# MAGIC
# MAGIC 1. Continue to add edge cases on affected tables: RESTORE TABLE, OPTIMIZE
# MAGIC 2. Ensure shapshot versions are created for tables that do not exists. if transaction fails and snapshot is -1, then run DROP TABLE IF EXISTS statement. 

# COMMAND ----------

# MAGIC %pip install -r helperfunctions/requirements.txt

# COMMAND ----------

from helperfunctions.transactions import Transaction

# COMMAND ----------

# DBTITLE 1,Example SQL Transaction Block
sqlString = """
USE CATALOG hive_metastore;

CREATE SCHEMA IF NOT EXISTS iot_dashboard;

USE SCHEMA iot_dashboard;

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

USE iot_dashboard;

-- This calculate table stats for all columns to ensure the optimizer can build the best plan
-- Statement 3

ANALYZE TABLE iot_dashboard.silver_sensors COMPUTE STATISTICS FOR ALL COLUMNS;

CREATE TABLE IF NOT EXISTS hourly_summary_statistics
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

# MAGIC %md
# MAGIC
# MAGIC ## 3 Primary Ways to Do a Transaction
# MAGIC
# MAGIC 1. <b> SQL - selected_tables: </b> This allows the user to explicitly control which exact tables get snapshotted and rolledback - good for production where lots of jobs are running. 
# MAGIC
# MAGIC 2. <b> SQL - inferred_selected_tables </b> This uses SQL Glot to automatically find tables that would be altered from the SQL inside the transaction block, and will snapshot those tables. Great for simplicity but should be checked in a test before moving to production
# MAGIC
# MAGIC 3. <b> Python </b> - call .begin_transaction() and rollback_transaction() methods manually do manage a transaction state. This allows for more complex logic outside of a contiguous multi statement SQL block

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Method 1: SQL - selected_tables

# COMMAND ----------

# DBTITLE 1,Initialize Transaction Class - Manually Define Selected Tables
x = Transaction(mode="selected_tables", uc_default=False)

# COMMAND ----------

# DBTITLE 1,Execute a multi statement SQL transaction from a SQL string - Manually Defining 
## This method is great because to do not need to rollback manually, it is handled for you
## This statement auto-commmits on success. If you do not want that, you can write pyspark or regular SQL outside of this method and then manually rollback
x.execute_sql_transaction(sqlString, tables_to_manage=["hive_metastore.iot_dashboard.silver_sensors"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 2: SQL - inferred_altered_tables

# COMMAND ----------

y = Transaction(mode="inferred_altered_tables", uc_default=False) ## uc_default=True if you want to infer schema with main as default instead of hive_metastore.

# COMMAND ----------

## This statement auto-commmits on success. If you do not want that, you can write pyspark or regular SQL outside of this method and then manually rollback


y.execute_sql_transaction(sqlString)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Method 3: Python
# MAGIC
# MAGIC Call transaction begin and rollback and do any logic in between

# COMMAND ----------

# DBTITLE 1,Begin Transaction in Python
x.begin_transaction(tables_to_snapshot=["hive_metastore.iot_dashbaord.silver_sensors"])

# COMMAND ----------

##### Do a bunch of logic here, any logic at all
#####

# COMMAND ----------

# DBTITLE 1,Get Transaction Snapshot Info
x.get_transaction_snapshot()

# COMMAND ----------

# DBTITLE 1,Manually rollback a transaction from most recent explicit snapshot for tables
### If you use the SQL execute method, it auto commits!! So you cannot roll back once it succeed. It will do it automatically. You can still use all the manual methods if you want to opt out of auto handling the rollback/committ process
x.rollback_transaction()
