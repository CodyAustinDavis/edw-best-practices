# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### This notebook takes the 2 groups of stats together, stack ranks all of them, and recommends an optimization plan for each table
# MAGIC 
# MAGIC ### Input Tables: 
# MAGIC 
# MAGIC 1. delta_optimizer.merge_predicate_statistics
# MAGIC 2. delta_optimizer.query_column_statistics - Column level query stats
# MAGIC 3. delta_optimizer.query_summary_statistics - Query level query stats
# MAGIC 4. delta_optimizer.raw_query_history_statistics - Raw Query History Stats
# MAGIC 
# MAGIC ## Roadmap: 
# MAGIC 
# MAGIC 1. Parameter that decides to optimize for write or read operations
# MAGIC 
# MAGIC 2. Parameter that decides how many columns to use
# MAGIC 
# MAGIC 3. Param that picks top N strategies
# MAGIC 
# MAGIC 4. Param that actually runs the best recommendations

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE delta_optimizer.column_level_summary_statistics
# MAGIC AS
# MAGIC WITH test_q AS (
# MAGIC SELECT * FROM delta_optimizer.query_column_statistics
# MAGIC WHERE length(ColumnName) >= 1 -- filter out queries with no joins or predicates
# MAGIC AND TableName = 'airbnb_demo_db.v_error_metrics'
# MAGIC ),
# MAGIC step_2 AS (
# MAGIC SELECT 
# MAGIC TableName,
# MAGIC ColumnName,
# MAGIC COUNT(DISTINCT query_id) AS QueryReferenceCount,
# MAGIC SUM(DurationTimesRuns) AS RawTotalRuntime,
# MAGIC AVG(AverageQueryDuration) AS AvgQueryDuration,
# MAGIC SUM(NumberOfColumnOccurrences) AS TotalColumnOccurrencesForAllQueries,
# MAGIC AVG(NumberOfColumnOccurrences) AS AvgColumnOccurrencesInQueryies
# MAGIC FROM test_q
# MAGIC WHERE length(ColumnName) >=1
# MAGIC GROUP BY TableName, ColumnName
# MAGIC )
# MAGIC -- Normalize statistics
# MAGIC SELECT 
# MAGIC *
# MAGIC FROM step_2
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Need to to min max normalization partitioned by table with 0 as the floor
# MAGIC SELECT PRECENTILE_RANK(approx_percentile)* FROM delta_optimizer.column_level_summary_statistics

# COMMAND ----------

from pyspark.ml.feature import MinMaxScaler, RobustScaler
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler

df = spark.sql("""SELECT * FROM delta_optimizer.column_level_summary_statistics""")

columns_to_scale = ["QueryReferenceCount", "RawTotalRuntime", "AvgQueryDuration", "TotalColumnOccurrencesForAllQueries", "AvgColumnOccurrencesInQueryies"]
assemblers = [VectorAssembler(inputCols=[col], outputCol=col + "_vec") for col in columns_to_scale]
scalers = [RobustScaler(inputCol=col + "_vec", outputCol=col + "_scaled") for col in columns_to_scale]
pipeline = Pipeline(stages=assemblers + scalers)
scalerModel = pipeline.fit(df)
scaledData = scalerModel.transform(df)


# COMMAND ----------

scaledData.createOrReplaceTempView("scaled_results")


# COMMAND ----------

from pyspark.ml.functions import vector_to_array


finalDf = (scaledData
           .withColumn("QueryReferenceCount_clean", vector_to_array("QueryReferenceCount_scaled")[0])
           .withColumn("RawTotalRuntime_clean", vector_to_array("RawTotalRuntime_scaled")[0])
           .withColumn("AvgQueryDuration_clean", vector_to_array("AvgQueryDuration_scaled")[0])
           .withColumn("TotalColumnOccurrencesForAllQueries_clean", vector_to_array("TotalColumnOccurrencesForAllQueries_scaled")[0])
           .withColumn("AvgColumnOccurrencesInQueryies_clean", vector_to_array("AvgColumnOccurrencesInQueryies_scaled")[0])
           .selectExpr("*", "(RawTotalRuntime_clean*TotalColumnOccurrencesForAllQueries_clean*QueryReferenceCount_clean + AvgColumnOccurrencesInQueryies_clean) AS FinalWeightScore")
          )

display(finalDf)


# COMMAND ----------

### Final ranking steps

## Calculate multiplied aggregate weighted score
## Normalized into Percentile
## Rank by weighted score and pick top 2 by default, and add more if above 50th percentile or something

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECt * fROM delta_optimizer.merge_predicate_statistics
