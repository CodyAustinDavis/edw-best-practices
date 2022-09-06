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
# MAGIC ### Output Tables: 
# MAGIC 
# MAGIC 1. delta_optimizer.final_ranked_cols_by_table -- Raw ranked columns by table
# MAGIC 2. delta_optimizer.final_optimize_config -- Config put into SQL String to be used by a job or DLT pipeline
# MAGIC 
# MAGIC ## Roadmap: 
# MAGIC 
# MAGIC <li> 1. Refine scoring mechanism to be more nuanced, including dynamically deciding how many columns to use depending on the distance between metrics
# MAGIC 
# MAGIC <li> 2. Add calculation for factoring in cardinality to the ranking strategy

# COMMAND ----------

dbutils.widgets.dropdown("optimizeMethod", "Both", ["Reads", "Writes", "Both"])
dbutils.widgets.dropdown("numZorderCols", "3", ["1","2","3","4","5"])
dbutils.widgets.text("Database", "All")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE delta_optimizer.column_level_summary_statistics
# MAGIC AS
# MAGIC WITH test_q AS (
# MAGIC SELECT * FROM delta_optimizer.query_column_statistics
# MAGIC WHERE length(ColumnName) >= 1 -- filter out queries with no joins or predicates
# MAGIC AND CASE WHEN "${Database}" != "All" THEN TableName LIKE (CONCAT("${Database}" ,"%")) ELSE true END
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
# MAGIC SELECT 
# MAGIC *
# MAGIC FROM step_2
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM delta_optimizer.column_level_summary_statistics

# COMMAND ----------

# DBTITLE 1,Min Max Normalization by Table
from pyspark.sql.functions import *

## This is the process for EACH table
df = spark.sql("""SELECT * FROM delta_optimizer.column_level_summary_statistics""")

columns_to_scale = ["QueryReferenceCount", "RawTotalRuntime", "AvgQueryDuration", "TotalColumnOccurrencesForAllQueries", "AvgColumnOccurrencesInQueryies"]
min_exprs = {x: "min" for x in columns_to_scale}
max_exprs = {x: "max" for x in columns_to_scale}

## Apply basic min max scaling by table for now

dfmin = df.groupBy("TableName").agg(min_exprs)
dfmax = df.groupBy("TableName").agg(max_exprs)

df_boundaries = dfmin.join(dfmax, on="TableName", how="inner")

df_pre_scaled = df.join(df_boundaries, on="TableName", how="inner")

df_scaled = (df_pre_scaled
         .withColumn("QueryRefernceCountScaled", coalesce((col("QueryReferenceCount") - col("min(QueryReferenceCount)"))/(col("max(QueryReferenceCount)") - col("min(QueryReferenceCount)")), lit(0)))
         .withColumn("RawTotalRuntimeScaled", coalesce((col("RawTotalRuntime") - col("min(RawTotalRuntime)"))/(col("max(RawTotalRuntime)") - col("min(RawTotalRuntime)")), lit(0)))
         .withColumn("AvgQueryDurationScaled", coalesce((col("AvgQueryDuration") - col("min(AvgQueryDuration)"))/(col("max(AvgQueryDuration)") - col("min(AvgQueryDuration)")), lit(0)))
         .withColumn("TotalColumnOccurrencesForAllQueriesScaled", coalesce((col("TotalColumnOccurrencesForAllQueries") - col("min(TotalColumnOccurrencesForAllQueries)"))/(col("max(TotalColumnOccurrencesForAllQueries)") - col("min(TotalColumnOccurrencesForAllQueries)")), lit(0)))
         .withColumn("AvgColumnOccurrencesInQueriesScaled", coalesce((col("AvgColumnOccurrencesInQueryies") - col("min(AvgColumnOccurrencesInQueryies)"))/(col("max(AvgColumnOccurrencesInQueryies)") - col("min(AvgColumnOccurrencesInQueryies)")), lit(0)))
            )

display(df_scaled)

# COMMAND ----------

"""
def scale_columns_by_table(df):
  
  columns_to_scale = ["QueryReferenceCount", "RawTotalRuntime", "AvgQueryDuration", "TotalColumnOccurrencesForAllQueries", "AvgColumnOccurrencesInQueryies"]
  assemblers = [VectorAssembler(inputCols=[col], outputCol=col + "_vec") for col in columns_to_scale]
  scalers = [RobustScaler(inputCol=col + "_vec", outputCol=col + "_scaled") for col in columns_to_scale]
  pipeline = Pipeline(stages=assemblers + scalers)
  scalerModel = pipeline.fit(df)
  scaledData = scalerModel.transform(df)
  
  return scaledData
"""

# COMMAND ----------

df_scaled.createOrReplaceTempView("scaled_results")

# COMMAND ----------

### Final ranking steps

## Calculate multiplied aggregate weighted score
## Normalized into Percentile
## Rank by weighted score and pick top 2 by default, and add more if above 50th percentile or something

# COMMAND ----------

# DBTITLE 1,Add Separate Score for Write Statistics and Save Final Rankings
# MAGIC %sql
# MAGIC -- TO DO: make more nuanced and intelligent for generality
# MAGIC -- Params: SELECT "${optimizeMethod}", "${numZorderCols}"
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE delta_optimizer.final_ranked_cols_by_table
# MAGIC AS 
# MAGIC WITH final_stats AS (
# MAGIC SELECT
# MAGIC COALESCE(reads.TableName, writes.TableName) AS TableName,
# MAGIC COALESCE(reads.ColumnName, writes.TableColumns) AS ColumnName,
# MAGIC QueryRefernceCountScaled,
# MAGIC RawTotalRuntimeScaled,
# MAGIC AvgQueryDurationScaled,
# MAGIC TotalColumnOccurrencesForAllQueriesScaled,
# MAGIC AvgColumnOccurrencesInQueriesScaled,
# MAGIC COALESCE(HasColumnInMergePredicate, 0) AS HasColumnInMergePredicate -- Not all tables will be MERGE targets
# MAGIC FROM scaled_results AS reads 
# MAGIC LEFT JOIN delta_optimizer.merge_predicate_statistics AS writes ON writes.TableName = reads.TableName AND writes.TableColumns = reads.ColumnName
# MAGIC ),
# MAGIC raw_scoring AS (
# MAGIC SELECT 
# MAGIC *,
# MAGIC CASE WHEN "${optimizeMethod}" = "Both" THEN QueryRefernceCountScaled + RawTotalRuntimeScaled + AvgQueryDurationScaled + TotalColumnOccurrencesForAllQueriesScaled + HasColumnInMergePredicate /*evenly weight merge predicate but add it in */
# MAGIC WHEN "${optimizeMethod}" = "Read" THEN QueryRefernceCountScaled + RawTotalRuntimeScaled + AvgQueryDurationScaled + TotalColumnOccurrencesForAllQueriesScaled /* If Read, do not add merge predicate to score */
# MAGIC WHEN "${optimizeMethod}" = "Write" THEN QueryRefernceCountScaled + RawTotalRuntimeScaled + AvgQueryDurationScaled + TotalColumnOccurrencesForAllQueriesScaled + 5*HasColumnInMergePredicate /* heavily weight the column such that it is always included in ZORDER , TO DO: Factor in cardinality here */
# MAGIC END AS RawScore
# MAGIC FROM final_stats
# MAGIC ),
# MAGIC -- Add cardinality in here somehow
# MAGIC ranked_scores AS (
# MAGIC SELECT 
# MAGIC *,
# MAGIC ROW_NUMBER() OVER( PARTITION BY TableName ORDER BY RawScore DESC) AS ColumnRank
# MAGIC FROM raw_scoring
# MAGIC )
# MAGIC 
# MAGIC SELECT 
# MAGIC *
# MAGIC FROM ranked_scores
# MAGIC WHERE ColumnRank <= "${numZorderCols}"::integer -- filter out max ZORDER cols, we will then collect list into OPTIMIZE string to run

# COMMAND ----------

final_df = spark.sql("""
WITH tt AS 
(
SELECT 
TableName, collect_list(ColumnName) AS ZorderCols
FROM delta_optimizer.final_ranked_cols_by_table
GROUP BY TableName
)
SELECT 
*,
concat("OPTIMIZE ", TableName, " ZORDER BY (", concat_ws(", ",ZorderCols), ");") AS ZOrderString
FROM tt
""")

### Save as single partition so collect is simple cause this should just be a config table
final_df.repartition(1).write.format("delta").mode("overwrite").saveAsTable("delta_optimizer.final_optimize_config")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM delta_optimizer.final_optimize_config

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Throw in ANALYZE Table into config
# MAGIC 
# MAGIC ## Franco says that anything with a predicate gets VERY much sped up where columns have a predicate (MERGE or highly Selective)
# MAGIC ## If you to SELECT * from a table with 1 MB is BAD so that is the tradeoff
# MAGIC 
# MAGIC -- Add in section 2 when you get it from the transaction log
# MAGIC 
# MAGIC -- Goal: Output is 1 daily scheduled job
