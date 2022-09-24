# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## This notebook uses the query history API on Databricks SQL to pull the query history for all users within the last X days and builds a SQL profile from the query text to find most key columns to ZORDER on
# MAGIC 
# MAGIC 
# MAGIC ### RETURNS
# MAGIC 
# MAGIC 1. delta_optimizer.query_column_statistics - Column level query stats
# MAGIC 2. delta_optimizer.query_summary_statistics - Query level query stats
# MAGIC 3. delta_optimizer.raw_query_history_statistics - Raw Query History Stats
# MAGIC 
# MAGIC ### Depedencies
# MAGIC <li> https://github.com/macbre/sql-metadata -- pip install sql-metadata
# MAGIC <li> Ensure that you either get a token as a secret or use a cluster with the env variable called DBX_TOKEN to authenticate to DBSQL
# MAGIC 
# MAGIC   
# MAGIC   
# MAGIC   
# MAGIC DBX_TOKEN = os.environ.get("DBX_TOKEN")

# COMMAND ----------

dbutils.widgets.dropdown("Query History Lookback Period (days)", defaultValue="3",choices=["1","3","7","14","30","60","90"])
dbutils.widgets.text("SQL Warehouse Ids (csv list)", "")
dbutils.widgets.text("Workspace DNS:", "")

# COMMAND ----------

lookbackPeriod = int(dbutils.widgets.get("Query History Lookback Period (days)"))
warehouseIdsList = [i.strip() for i in dbutils.widgets.get("SQL Warehouse Ids (csv list)").split(",")]
workspaceName = dbutils.widgets.get("Workspace DNS:").strip()
warehouse_ids = dbutils.widgets.get("SQL Warehouse Ids (csv list)")
print(f"Loading Query Profile to delta from workspace: {workspaceName} \n from Warehouse Ids: {warehouseIdsList} \n for the last {lookbackPeriod} days...")

# COMMAND ----------

DBX_TOKEN = os.environ.get("DBX_TOKEN")

print(f"Loading Query Profile to delta from workspace: {workspaceName} from Warehouse Ids: {warehouseIdsList} for the last {lookbackPeriod} days...")


# COMMAND ----------

# MAGIC %pip install sqlparse
# MAGIC %pip install sql-metadata
