# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <h1> Silver to Gold: Analytics of IoT stream tables </h1>
# MAGIC 
# MAGIC This notebook uses any preferred analyst language to quickly get insights from the tables Data Engineering just streamed in!

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE codydemos;

# COMMAND ----------

# MAGIC %md 
# MAGIC <b> Analyze Air Temp Trends </b>

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW IF NOT EXISTS air_temp_ma_live
# MAGIC AS
# MAGIC SELECT *,
# MAGIC avg(`SensorValue`) OVER (
# MAGIC         ORDER BY `MeasurementDateTime`
# MAGIC         ROWS BETWEEN
# MAGIC           30 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ) AS TempShortMovingAverage,
# MAGIC       
# MAGIC avg(`SensorValue`) OVER (
# MAGIC         ORDER BY `MeasurementDateTime`
# MAGIC         ROWS BETWEEN
# MAGIC           365 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ) AS TempLongMovingAverage
# MAGIC FROM silver_airtempsensor;
