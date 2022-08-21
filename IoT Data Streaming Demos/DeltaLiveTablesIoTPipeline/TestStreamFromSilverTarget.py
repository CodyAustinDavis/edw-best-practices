# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.readStream.table("codydltdemos.silver_all_sensors")
