# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### This notebook takes the 2 groups of stats together, stack ranks all of them, and recommends an optimization plan for each table
# MAGIC 
# MAGIC ### Input Tables: 
# MAGIC 
# MAGIC 1. delta_optimizer.merge_predicate_statistics - Column level Write stats
# MAGIC 2. delta_optimizer.query_column_statistics - Column level query stats
# MAGIC 3. delta_optimizer.query_summary_statistics - Query level query stats
# MAGIC 4. delta_optimizer.raw_query_history_statistics - Raw Query History Stats
# MAGIC 5. delta_optimizer.all_tables_cardinality_stats - All tables from selected database, with or without cardinality stats
# MAGIC 
# MAGIC ### Output Tables: 
# MAGIC 
# MAGIC 1. delta_optimizer.final_ranked_cols_by_table -- Raw ranked columns by table
# MAGIC 2. <b> delta_optimizer.final_optimize_config </b> -- Config put into SQL String to be used by a job or DLT pipeline
# MAGIC 
# MAGIC ## Roadmap: 
# MAGIC <ul> 
# MAGIC <li> 1. Refine scoring mechanism to be more nuanced, including dynamically deciding how many columns to use depending on the distance between metrics
# MAGIC 
# MAGIC <li> 2. Add calculation for factoring in cardinality to the ranking strategy - <b> DONE - MVP </b>
# MAGIC   
# MAGIC <li> 3. Add COMPUTE STATISTICS STATEMENT BASED ON TABLE SIZE -<b> DONE - MVP </b> 
# MAGIC   
# MAGIC <li> 4. Add ALTER TABLE STATEMENT Based on file size and merge predicate mappings -<b> DONE - MVP </b>
# MAGIC   
# MAGIC <li> 5. Add Column re-ordering for tables that need to be analyzed (explicitly only, or ZORDERed) <b> IN PROGRESS - MVP </b>
# MAGIC   
# MAGIC </ul>

# COMMAND ----------

from pyspark.sql.functions import *
import re

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

    ## This query ensures that it does not scan the whole table and THEN limits
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
# MAGIC ### Parse Cardinality Stats

# COMMAND ----------

cardinality_statement = df_cardinality.collect()
cardinality_config = {i[0]: {"columns": i[1], "sql": i[2]} for i in cardinality_statement}

# COMMAND ----------

# DBTITLE 1,Build Cardinality Stats for All Tables Where columns are used in read or write predicates
for i in cardinality_config:
    try:
        
        print(f"Building Cardinality Statistics for {i} ... \n")
        
        ## Gets cardinality stats on tables at a time, but all columns in parallel, then pivots results to long form
        wide_df = (spark.sql(cardinality_config.get(i).get("sql")))
        table_name = i
        clean_list = [ "'" + re.search('[^_]*_(.*)', i).group(1) + "'" + ", " + i for i in wide_df.columns if re.search('[^_]*_(.*)', i) is not None]
        clean_expr = ", ".join(clean_list)
        unpivot_Expr = f"stack({len(clean_list)}, {clean_expr}) as (ColumnName,ColumnDistinctCount)"	

        unpivot_DataFrame = (wide_df.select(expr(unpivot_Expr), "TotalCount").withColumn("TableName", lit(table_name))
                             .withColumn("CardinalityProportion", col("ColumnDistinctCount").cast("double")/col("TotalCount").cast("double"))
                            )

        ## Standard Mix/Max Scale Proportion
        columns_to_scale = ["CardinalityProportion"]
        min_exprs = {x: "min" for x in columns_to_scale}
        max_exprs = {x: "max" for x in columns_to_scale}

        ## Apply basic min max scaling by table for now

        dfmin = unpivot_DataFrame.groupBy("TableName").agg(min_exprs)
        dfmax = unpivot_DataFrame.groupBy("TableName").agg(max_exprs)

        df_boundaries = dfmin.join(dfmax, on="TableName", how="inner")

        df_pre_scaled = unpivot_DataFrame.join(df_boundaries, on="TableName", how="inner")

        df_scaled = (df_pre_scaled
                 .withColumn("CardinalityScaled", coalesce((col("CardinalityProportion") - col("min(CardinalityProportion)"))/(col("max(CardinalityProportion)") - col("min(CardinalityProportion)")), lit(0)))
                    )

        #display(df_scaled.orderBy("TableName"))

        df_scaled.createOrReplaceTempView("card_stats")

        spark.sql(f"""
            MERGE INTO delta_optimizer.all_tables_cardinality_stats AS target 
            USING card_stats AS source ON source.TableName = target.TableName AND source.ColumnName = target.ColumnName
            WHEN MATCHED THEN UPDATE SET
            target.SampleSize = CAST({cardinalitySampleSize} AS INTEGER),
            target.TotalCountInSample = source.TotalCount,
            target.DistinctCountOfColumnInSample = source.ColumnDistinctCount,
            target.CardinalityProportion = (CAST(ColumnDistinctCount AS DOUBLE) / CAST(TotalCount AS DOUBLE)),
            target.CardinalityProportionScaled = source.CardinalityScaled::double
        """)
        
    except Exception as e:
        print(f"Skipping table {i} due to error {str(e)} \n")
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final ranking steps
# MAGIC 
# MAGIC <li> Calculate multiplied aggregate weighted score
# MAGIC <li> Normalized into Percentile
# MAGIC <li> Rank by weighted score and pick top 2 by default, and add more if above 50th percentile or something

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
CASE WHEN array_contains(tls.partitionColumns, spine.ColumnName) THEN 1 ELSE 0 END AS IsPartitionCol,
QueryReferenceCountScaled,
RawTotalRuntimeScaled,
AvgQueryDurationScaled,
TotalColumnOccurrencesForAllQueriesScaled,
AvgColumnOccurrencesInQueriesScaled,
isUsedInJoin,
isUsedInFilter,
isUsedInGroup
FROM delta_optimizer.all_tables_cardinality_stats AS spine
LEFT JOIN delta_optimizer.all_tables_table_stats AS tls ON tls.TableName = spine.TableName
LEFT JOIN delta_optimizer.read_statistics_scaled_results AS reads ON spine.TableName = reads.TableName AND spine.ColumnName = reads.ColumnName
),
raw_scoring AS (
-- THIS IS THE CORE SCORING EQUATION
SELECT 
*,
 CASE WHEN IsPartitionCol = 1 THEN 0 
 ELSE 
     CASE 
     WHEN '{optimizeMethod}' = "Both" 
           THEN IsUsedInReads*(1 + COALESCE(QueryReferenceCountScaled,0) + COALESCE(RawTotalRuntimeScaled,0) + COALESCE(AvgQueryDurationScaled, 0) + COALESCE(TotalColumnOccurrencesForAllQueriesScaled, 0) + COALESCE(isUsedInFilter,0) + COALESCE(isUsedInJoin,0) + COALESCE(isUsedInGroup, 0))*(0.001+ COALESCE(CardinalityProportionScaled,0)) + (IsUsedInWrites*(0.001+COALESCE(CardinalityProportionScaled, 0)))
     WHEN '{optimizeMethod}' = "Read" 
           THEN IsUsedInReads*(1 + COALESCE(QueryReferenceCountScaled,0) + COALESCE(RawTotalRuntimeScaled,0) + COALESCE(AvgQueryDurationScaled, 0) + COALESCE(TotalColumnOccurrencesForAllQueriesScaled, 0) + COALESCE(isUsedInFilter,0) + COALESCE(isUsedInJoin,0) + COALESCE(isUsedInGroup, 0))*(0.001+ COALESCE(CardinalityProportionScaled,0)) /* If Read, do not add merge predicate to score */
    WHEN '{optimizeMethod}' = "Write" 
            THEN IsUsedInReads*(1 + COALESCE(QueryReferenceCountScaled,0) + COALESCE(RawTotalRuntimeScaled,0) + COALESCE(AvgQueryDurationScaled, 0) + COALESCE(TotalColumnOccurrencesForAllQueriesScaled, 0) + COALESCE(isUsedInFilter,0) + COALESCE(isUsedInJoin,0) + COALESCE(isUsedInGroup, 0))*(0.001+ COALESCE(CardinalityProportionScaled,0)) + (5*IsUsedInWrites*(0.001+COALESCE(CardinalityProportionScaled, 0))) /* heavily weight the column such that it is always included */
    END
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

# DBTITLE 1,Create Final Config Table Map
spark.sql("""CREATE TABLE IF NOT EXISTS  delta_optimizer.final_optimize_config
        (TableName STRING NOT NULL,
        ZorderCols ARRAY<STRING>,
        OptimizeCommandString STRING, --OPTIMIZE OPTIONAL < ZORDER BY >
        AlterTableCommandString STRING, --delta.targetFileSize, delta.tuneFileSizesForRewrites
        AnalyzeTableCommandString STRING -- ANALYZE TABLE COMPUTE STATISTICS
        )
""")

# COMMAND ----------

final_df = (spark.sql("""
  WITH tt AS 
  (
      SELECT 
      TableName, collect_list(ColumnName) AS ZorderCols
      FROM delta_optimizer.final_ranked_cols_by_table
      GROUP BY TableName
  )
  SELECT 
  *,
  CASE WHEN size(ZorderCols) >=1 
          THEN concat("OPTIMIZE ", TableName, " ZORDER BY (", concat_ws(", ",ZorderCols), ");")
      ELSE concat("OPTIMIZE ", TableName, ";")
      END AS OptimizeCommandString,
  NULL AS AlterTableCommandString,
  NULL AS AnalyzeTableCommandString
  FROM tt
""")
           )

### Save as single partition so collect is simple cause this should just be a config table
final_df.repartition(1).write.format("delta").mode("overwrite").saveAsTable("delta_optimizer.final_optimize_config")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Calculate ANALYZE TABLE Expression
# MAGIC 
# MAGIC <p1> Build an ANALYZE TABLE command with the following heuristics: 
# MAGIC </p1>
# MAGIC 
# MAGIC <li> 1. IF: table less than 100GB Run COMPUTE STATISTICS FOR ALL COLUMNS
# MAGIC <li> 2. ELSE IF: table great than 100GB, run COMPUTE STATISTICS FOR COLUMNS used in GROUP, FILTER, OR JOINS ONLY

# COMMAND ----------

# DBTITLE 1,Define functions to build ANALYZE and ALTER TABLE SQL Expression
@udf("string")
def getAnalyzeTableCommand(inputTableName, tableSizeInGb, relativeColumns):
    
    ### Really basic heuristic to calculate statistics, can increase nuance in future versions
    tableSizeInGbLocal = float(tableSizeInGb)
    
    if tableSizeInGbLocal <= 100:
        sqlExpr = f"ANALYZE TABLE {inputTableName} COMPUTE STATISTICS FOR ALL COLUMNS;"
        return sqlExpr
    else:
        
        rel_Cols = str(relativeColumns).split(",")
        colExpr = ", ".join(rel_Cols)
        sqlExpr = f"ANALYZE TABLE {inputTableName} COMPUTE STATISTICS FOR COLUMNS {colExpr};"
        return sqlExpr
    
    
@udf("string")   
def getAlterTableCommand(inputTableName, fileSizeMapInMb, isMergeUsed):
    
    if float(isMergeUsed) >=1:
        
        alterExpr = f"ALTER TABLE {inputTableName} SET TBLPROPERTIES ('delta.targetFileSize' = '{fileSizeMapInMb}', 'delta.tuneFileSizesForRewrites' = 'true');"
        return alterExpr
    else: 
        alterExpr = f"ALTER TABLE {inputTableName} SET TBLPROPERTIES ('delta.targetFileSize' = '{fileSizeMapInMb}', 'delta.tuneFileSizesForRewrites' = 'false');"
        return alterExpr

# COMMAND ----------

# DBTITLE 1,Build Analyze Stats and TBL PROPERTIES Commands for All Tables
analyze_stats_df = (spark.sql("""
        WITH stats_cols AS (
        SELECT DISTINCT
        spine.TableName,
        card_stats.ColumnName
        FROM delta_optimizer.all_tables_table_stats spine
        LEFT JOIN delta_optimizer.all_tables_cardinality_stats AS card_stats ON card_stats.TableName = spine.TableName
        LEFT JOIN delta_optimizer.read_statistics_scaled_results AS reads ON card_stats.TableName = reads.TableName AND reads.ColumnName = card_stats.ColumnName
        WHERE card_stats.IsUsedInWrites = 1
            OR (reads.isUsedInJoin + reads.isUsedInFilter + reads.isUsedInGroup ) >= 1
        )
        SELECT 
        spine.TableName,
        MAX(spine.sizeInGB) AS sizeInGB,
        MAX(spine.mappedFileSizeInMb) AS fileSizeMap,
        CASE WHEN MAX(IsUsedInWrites)::integer >= 1 THEN 1 ELSE 0 END AS ColumnsUsedInMerges, -- If table has ANY columns used in a merge predicate, tune file sizes for re-writes
        concat_ws(',', array_distinct(collect_list(reads.ColumnName)))  AS ColumnsToCollectStatsOn
        FROM delta_optimizer.all_tables_table_stats spine
        LEFT JOIN delta_optimizer.all_tables_cardinality_stats AS card_stats ON card_stats.TableName = spine.TableName
        LEFT JOIN stats_cols AS reads ON card_stats.TableName = reads.TableName AND reads.ColumnName = card_stats.ColumnName
        GROUP BY spine.TableName
        """)
                   )

analyze_stats_completed = (analyze_stats_df
                           .withColumn("AlterTableCommandString", getAlterTableCommand(col("TableName"), col("fileSizeMap"), col("ColumnsUsedInMerges")))
                           .withColumn("AnalyzeTableCommandString", getAnalyzeTableCommand(col("TableName"), col("sizeInGB"), col("ColumnsToCollectStatsOn")))
                          )

analyze_stats_completed.createOrReplaceTempView("analyze_stats")

spark.sql("""MERGE INTO delta_optimizer.final_optimize_config AS target
                USING analyze_stats AS source
                ON source.TableName = target.TableName
                WHEN MATCHED THEN 
                UPDATE SET 
                target.AlterTableCommandString = source.AlterTableCommandString,
                target.AnalyzeTableCommandString = source.AnalyzeTableCommandString
          """)
