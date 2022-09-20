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

from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.dropdown("optimizeMethod", "Both", ["Reads", "Writes", "Both"])
dbutils.widgets.dropdown("numZorderCols", "3", ["1","2","3","4","5"])
dbutils.widgets.text("Database", "All")
dbutils.widgets.dropdown("CardinalitySampleSize", "1000000", ["1000", "100000", "1000000", "10000000"])

# COMMAND ----------

cardinalitySampleSize = int(dbutils.widgets.get("CardinalitySampleSize"))

print(f"Cardinality Sample Size: {cardinalitySampleSize}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Calculate Cardinality Stats on All columns that appear in reads OR writes

# COMMAND ----------

# DBTITLE 1,Build String to efficiently calculate cardinality on a sample
@udf("string")
def buildCardinalitySampleSQLStatement(tableName, columnList, sampleSize:float):


    sampleString = f"WITH sample AS (SELECT * FROM {tableName} LIMIT {sampleSize})"
    sqlFrom = f" FROM sample"
    str2 = [" SELECT COUNT(0) AS TotalCount"]

    for i in columnList:
        sqlStr = f"COUNT(DISTINCT {i}) AS DistinctCountOf_{i}"
        str2.append(sqlStr)


    finalSql = sampleString + ", ".join(str2) + sqlFrom
    
    return finalSql

# COMMAND ----------

# DBTITLE 1,Check and Track Relevant Columns for Cardinality Stats
spark.sql("""WITH filter_cols AS (
    SELECT DISTINCT
    spine.TableName,
    spine.ColumnName,
    CASE WHEN reads.QueryReferenceCount >= 1 THEN 1 ELSE 0 END AS IsUsedInReads,
    CASE WHEN writes.HasColumnInMergePredicate >= 1 THEN 1 ELSE 0 END AS IsUsedInWrites
    FROM delta_optimizer.all_tables_cardinality_stats AS spine
    LEFT JOIN delta_optimizer.read_statistics_scaled_results reads ON spine.TableName = reads.TableName AND spine.ColumnName = reads.ColumnName
    LEFT JOIN delta_optimizer.write_statistics_merge_predicate writes ON spine.TableName = writes.TableName AND spine.ColumnName = writes.ColumnName
    )
MERGE INTO delta_optimizer.all_tables_cardinality_stats AS target
USING filter_cols AS source ON source.TableName = target.TableName AND source.ColumnName = target.ColumnName
WHEN MATCHED THEN UPDATE SET
target.IsUsedInReads = source.IsUsedInReads,
target.IsUsedInWrites = source.IsUsedInWrites
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM delta_optimizer.all_tables_cardinality_stats WHERE TableName = 'detection.viewing_commercials_firehose'

# COMMAND ----------

df_cardinality = (
    spark.sql("""
        SELECT TableName, collect_list(ColumnName) AS ColumnList
        FROM delta_optimizer.all_tables_cardinality_stats
        WHERE (IsUsedInReads > 0 OR IsUsedInWrites > 0) --If columns is not used in any joins or predicates, lets not do cardinality stats
        GROUP BY TableName
    """)
    .withColumn("cardinalityStatsStatement", buildCardinalitySampleSQLStatement(col("TableName"), col("ColumnList"), lit(cardinalitySampleSize))) # Take sample size of 1M, if table is smaller, index on the count
)

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Parse Cardinality Stats

# COMMAND ----------

cardinality_statement = df_cardinality.collect()
cardinality_config = {i[0]: {"columns": i[1], "sql": i[2]} for i in cardinality_statement}

# COMMAND ----------

# DBTITLE 1,Build Cardinality Stats for All Tables Where columns are used in read or write predicates
for i in cardinality_config:
    try:
        
        print(f"Building Cardinality Statistics for {i} ... \n")
        
        wide_df = (spark.sql(cardinality_config.get(i).get("sql")))
        table_name = i
        clean_list = [ "'" + re.search('[^_]*_(.*)', i).group(1) + "'" + ", " + i for i in wide_df.columns if re.search('[^_]*_(.*)', i) is not None]
        clean_expr = ", ".join(clean_list)
        unpivot_Expr = f"stack({len(clean_list)}, {clean_expr}) as (ColumnName,ColumnDistinctCount)"	

        unpivot_DataFrame = wide_df.select(expr(unpivot_Expr), "TotalCount").withColumn("TableName", lit(table_name))


        unpivot_DataFrame.createOrReplaceTempView("card_stats")

        spark.sql(f"""
            MERGE INTO delta_optimizer.all_tables_cardinality_stats AS target 
            USING card_stats AS source ON source.TableName = target.TableName AND source.ColumnName = target.ColumnName
            WHEN MATCHED THEN UPDATE SET
            target.SampleSize = CAST({cardinalitySampleSize} AS INTEGER),
            target.TotalCountInSample = source.TotalCount,
            target.DistinctCountOfColumnInSample = source.ColumnDistinctCount,
            target.CardinalityProportion = (CAST(ColumnDistinctCount AS DOUBLE) / CAST(TotalCount AS DOUBLE))
        """)
        
    except Exception as e:
        print(f"Skipping table {i} due to error {str(e)}")
        pass

# COMMAND ----------

### Final ranking steps

## Calculate multiplied aggregate weighted score
## Normalized into Percentile
## Rank by weighted score and pick top 2 by default, and add more if above 50th percentile or something

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM delta_optimizer.read_statistics_scaled_results WHERE TableName = 'detection.viewing_commercials_firehose'

# COMMAND ----------

optimizeMethod = dbutils.widgets.get("optimizeMethod")
numZorderCols = dbutils.widgets.get("numZorderCols")

# COMMAND ----------

# DBTITLE 1,Add Separate Score for Write Statistics and Save Final Rankings
spark.sql(f"""CREATE OR REPLACE TABLE delta_optimizer.final_ranked_cols_by_table
AS  (
WITH final_stats AS (
SELECT
spine.*,
QueryReferenceCountScaled,
RawTotalRuntimeScaled,
AvgQueryDurationScaled,
TotalColumnOccurrencesForAllQueriesScaled,
AvgColumnOccurrencesInQueriesScaled,
isUsedInJoin,
isUsedInFilter,
isUsedInGroup
FROM delta_optimizer.all_tables_cardinality_stats AS spine
LEFT JOIN delta_optimizer.read_statistics_scaled_results AS reads ON spine.TableName = reads.TableName AND spine.ColumnName = reads.ColumnName
),
raw_scoring AS (
-- THIS IS THE CORE SCORING EQUATION
SELECT 
*,
 CASE WHEN '{optimizeMethod}' = "Both" THEN (((1+QueryReferenceCountScaled)*(1+RawTotalRuntimeScaled)*(1+AvgQueryDurationScaled)*(1+TotalColumnOccurrencesForAllQueriesScaled)) + 100*(IsUsedInFilter) + IsUsedInJoin + 50*IsUsedInWrites)*(CardinalityProportion) /*evenly weight merge predicate but add it in */
WHEN '{optimizeMethod}' = "Read" THEN QueryReferenceCountScaled + RawTotalRuntimeScaled + AvgQueryDurationScaled + TotalColumnOccurrencesForAllQueriesScaled /* If Read, do not add merge predicate to score */
WHEN '{optimizeMethod}' = "Write" THEN QueryReferenceCountScaled + RawTotalRuntimeScaled + AvgQueryDurationScaled + TotalColumnOccurrencesForAllQueriesScaled + 5*IsUsedInWrites /* heavily weight the column such that it is always included */
END AS RawScore
FROM final_stats
),
-- Add cardinality in here somehow
ranked_scores AS (
SELECT 
*,
ROW_NUMBER() OVER( PARTITION BY TableName ORDER BY RawScore DESC) AS ColumnRank
FROM raw_scoring
)

SELECT 
*
FROM ranked_scores
WHERE ColumnRank <= {numZorderCols}::integer AND (IsUsedInReads + IsUsedInWrites) >= 1
OR (CardinalityProportion >= 0.2 AND RawScore IS NOT NULL) -- filter out max ZORDER cols, we will then collect list into OPTIMIZE string to run
ORDER BY TableName, ColumnRank
    )
""")

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
