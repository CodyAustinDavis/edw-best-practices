# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## This notebook provides functionality for multi-statement transactions
# MAGIC 
# MAGIC ### CAUTION: THIS DOES NOT SUPPORT MULTIPLE WRITERS, IF A TRANSACTION ROLLSBACK IT WILL REMOVE PROGRESS FROM ALL STATEMENTS THAT CHANGED EACH TABLE SUPPLIED BETWEEN SNAPSHOT AND ROLLBACK
# MAGIC <li> 1. Pass in the SQL transaction and the class will make sure all your tables are rolled back if any in the statement fail, updates snap_shot if success
# MAGIC <li> 2. Do not need to use SQL only (even though passing in a SQL string it the easiest)
# MAGIC <li> 3. Can create a begin a transaction with x.begin_transaction(table_list=[<list_of_tables_to_monitor>]) 
# MAGIC <li> 4. Committing the transaction simply updates the version of the snapshot with commit_transaction() of existing tables in the table monitoring list
# MAGIC <li> 5. Can manually rollback a transaction at any point with x.rollback_transaction() to most recent snapshot

# COMMAND ----------

# MAGIC %pip install -r helperfunctions/requirements.txt

# COMMAND ----------

from helperfunctions.deltahelpers import Transaction

# COMMAND ----------

sqlString = """
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

-- This calculate table stats for all columns to ensure the optimizer can build the best plan
-- Statement 3
ANALYZE TABLE iot_dashboard.silver_sensors COMPUTE STATISTICS FOR ALL COLUMNS;

-- Statement 4
-- Truncate bronze batch once successfully loaded
TRUNCATE TABLE iot_dashboard.bronze_sensors;
"""

# COMMAND ----------

# DBTITLE 1,Initialize Transaction Class
x = Transaction()

# COMMAND ----------

# DBTITLE 1,Save a Snapshot
x.begin_transaction(["iot_dashboard.silver_sensors", "iot_dashboard.bronze_sensors"])

# COMMAND ----------

# DBTITLE 1,Execute a multi statement SQL transaction from a SQL string
## This method is great because to do not need to rollback manually, it is handled for you
## This statement auto-commmits on success. If you do not want that, you can write pyspark or regular SQL outside of this method and then manually rollback
x.execute_sql_transaction(sqlString)

# COMMAND ----------

x.get_transaction_snapshot()

# COMMAND ----------

# DBTITLE 1,Manually rollback a transaction from most recent explicit snapshot for tables
### If you use the SQL execute method, it auto commits!! So you cannot roll back once it succeed. It will do it automatically. You can still use all the manual methods if you want to opt out of auto handling the rollback/committ process
x.rollback_transaction()
