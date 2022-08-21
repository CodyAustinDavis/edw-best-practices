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
# MAGIC CREATE VIEW air_temp_ma_live
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

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS gold_airtempanalysis;
# MAGIC 
# MAGIC CREATE TABLE gold_airtempanalysis 
# MAGIC USING DELTA 
# MAGIC AS (
# MAGIC 
# MAGIC   SELECT *,
# MAGIC   avg(`SensorValue`) OVER (
# MAGIC           ORDER BY `MeasurementDateTime`
# MAGIC           ROWS BETWEEN
# MAGIC             30 PRECEDING AND
# MAGIC             CURRENT ROW
# MAGIC         ) AS TempShortMovingAverage,
# MAGIC 
# MAGIC   avg(`SensorValue`) OVER (
# MAGIC           ORDER BY `MeasurementDateTime`
# MAGIC           ROWS BETWEEN
# MAGIC             365 PRECEDING AND
# MAGIC             CURRENT ROW
# MAGIC         ) AS TempLongMovingAverage
# MAGIC   FROM silver_airtempsensor
# MAGIC );

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT *,
# MAGIC avg(`SensorValue`) OVER (
# MAGIC         ORDER BY `MeasurementDateTime`
# MAGIC         ROWS BETWEEN
# MAGIC           30 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ) AS WaterTempShortMovingAverage,
# MAGIC       
# MAGIC avg(`SensorValue`) OVER (
# MAGIC         ORDER BY `MeasurementDateTime`
# MAGIC         ROWS BETWEEN
# MAGIC           365 PRECEDING AND
# MAGIC           CURRENT ROW
# MAGIC       ) AS WaterTempLongMovingAverage
# MAGIC FROM silver_waterqualitysensor;

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS gold_waterqualityanalysis;
# MAGIC 
# MAGIC CREATE TABLE gold_waterqualityanalysis
# MAGIC USING DELTA
# MAGIC AS (
# MAGIC   SELECT *,
# MAGIC   avg(`SensorValue`) OVER (
# MAGIC           ORDER BY `MeasurementDateTime`
# MAGIC           ROWS BETWEEN
# MAGIC             30 PRECEDING AND
# MAGIC             CURRENT ROW
# MAGIC         ) AS WaterTempShortMovingAverage,
# MAGIC 
# MAGIC   avg(`SensorValue`) OVER (
# MAGIC           ORDER BY `MeasurementDateTime`
# MAGIC           ROWS BETWEEN
# MAGIC             365 PRECEDING AND
# MAGIC             CURRENT ROW
# MAGIC         ) AS WaterTempLongMovingAverage
# MAGIC   FROM silver_waterqualitysensor
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2> Overall Value </h2>
# MAGIC 
# MAGIC <li> Quicker time to insights </li>
# MAGIC <li> Increased Collaboration between teams </li>
# MAGIC <li> Embedded SQL </li>
# MAGIC <li> More transparent Data Quality Monitoring </li>
