# Databricks notebook source
from pyspark.sql.functions import *

class DeltaMergeHelpers():

    def __init__(self):
        pass

    @staticmethod
    def retrySqlStatement(operationName, sqlStatement, maxRetries = 5):

        import time
        maxRetries = maxRetries
        numRetries = 0
        maxWaitTime = 1
        ### Does not check for existence, ensure that happens before merge

        while numRetries <= maxRetries:

            try: 

                print(f"SQL Statement Attempt for {operationName} #{numRetries + 1}...")

                spark.sql(sqlStatement)

                print(f"SQL Statement Attempt for {operationName} #{numRetries + 1} Successful!")
                break

            except Exception as e:
                error_msg = str(e)

                print(f"Failed SQL Statment Attmpet for {operationName} #{numRetries} with error: {error_msg}")

                numRetries += 1
                if numRetries > maxRetries:
                    break

            waitTime = 1 ## Constant wait time of one second, for pipelines with long merges, you may want to add a variable function to increase this as it retries
            ## Dont increase wait time infinitely 

            if waitTime > maxWaitTime:
                waitTime = maxWaitTime

            print(f"Waiting {waitTime} seconds before next attempt on {operationName}...")
            time.sleep(waitTime)


# COMMAND ----------

class DeltaHelpers():
    
    def __init__(self, temp_root_path, db_name="helpers_temp"):
        
        self.db_name = db_name
        self.temp_root_path = temp_root_path
        self.session_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        self.temp_env = self.temp_root_path + self.session_id
        
        spark.sql(f"""CREATE DATABASE IF NOT EXISTS {self.db_name}""")
        print(f"Initializing Root Temp Environment: {self.temp_env}")
        return
    
    def saveToDeltaTempTable(self, df, table_name):
        
        tblObj = {}
        new_table_id = table_name
        write_path = self.temp_env + new_table_id
        
        spark.sql(f"DROP TABLE IF EXISTS {self.db_name}.{new_table_id}")
        dbutils.fs.rm(write_path, recurse=True)
        ## TO DO: Add a create or replace function
        df.write.format("delta").mode("overwrite").option("path", write_path).saveAsTable(f"{self.db_name}.{new_table_id}")
        
        persisted_df = spark.read.format("delta").load(write_path)
        return persisted_df
        
    def removeDeltaTempTable(self, table_name):
        
        table_path = self.temp_env + table_name
        dbutils.fs.rm(table_path, recurse=True)
        spark.sql(f"""DROP TABLE IF EXISTS {self.db_name}.{table_name}""")
        
        print(f"Temp Table: {table_name} has been deleted.")
        return
    
    def removeAllTempTablesForSession(self):
        
        dbutils.fs.rm(self.temp_env, recurse=True)
        ##spark.sql(f"""DROP DATABASE IF EXISTS {self.db_name} CASCADE""") This temp db name COULD be global, never delete without separate method
        print(f"All temp tables in the session have been removed: {self.temp_env}")
        return
        
