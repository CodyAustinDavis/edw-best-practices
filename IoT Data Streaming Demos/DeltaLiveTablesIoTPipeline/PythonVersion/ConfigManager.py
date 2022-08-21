# Databricks notebook source
import json


json_config = {'water_quality': 'h2o_quality',
                 'air_temp':'average_temperature',
                'water_ph':'h2o_pH',
                'water_temp':'h2o_temperature'
                }

# COMMAND ----------

### Open JSON
with open("/dbfs/FileStore/shared_uploads/cody.davis@databricks.com/dlt_config") as con:
  x = json.load(con)

# COMMAND ----------

## Do changes to json and config with or without spark

print(x)

# COMMAND ----------

### Write JSON config back
