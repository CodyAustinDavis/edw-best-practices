# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Run the output of recommended optimize statements as a single run or schedule as a periodic job
# MAGIC 
# MAGIC #### Roadmap: 
# MAGIC 
# MAGIC 1. Use DLT to auto optimize LIVE and Normal Delta Tables if possible
# MAGIC 2. Use DLT metaprogramming framework to run in parallel (performance implications)
# MAGIC 3. Use Jobs API to automatically set up a daily / hourly job for this. This is NOT always recommended by default. The optimize timing greatly depends on the ETL pipelines

# COMMAND ----------

## This table by default has only 1 file, so it shouldnt be expensive to collect
config_row = spark.sql("""SELECT ZOrderString FROM delta_optimizer.final_optimize_config""").collect()
config = [i[0] for i in config_row]
print(f"Running {len(config)} OPTIMIZE commands: \n {config}")

# COMMAND ----------

# DBTITLE 1,Run through config and optimize
for i in config:
  print(f"Running OPTIMIZE command...")
  try: 
    spark.sql(i)
    print(f"Completed OPTIMIZE command for {i}")
  except Exception as e:
    print(f"Optimize failed with error: {str(e)}")
  
