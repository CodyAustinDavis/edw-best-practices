# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

def validate_json(input_json):
  malformed_json_flag='N'
  malformed_json=''
  try:
    json.loads(input_json)
  except:
    malformed_json=input_json
    malformed_json_flag='Y'
  return malformed_json

ValidateJSON = udf(lambda y: validate_json(y),StringType())


# COMMAND ----------

#### Register udf for generating UUIDs
import uuid

@udf(StringType())
def uuidUdf():
  x = uuid.uuid4()
  return x

spark.udf.register("uuidUdf", uuidUdf) # register the square udf for Spark SQL
