from helperfunctions.dbsqlclient import *
from helperfunctions.transactions import Transaction


class DBSQLTransactionManager(ServerlessClient, Transaction):

  def __init__(self, warehouse_id, mode="selected_tables", uc_default=False, host_name=None, token=None):

    super().__init__(self, warehouse_id=warehouse_id, host_name=host_name, token=token, mode=mode, uc_default=uc_default)

    return
  

### Execute multi statment SQL, now we can implement this easier for Serverless or not Serverless
def execute_sql_transaction(self, sql_string, tables_to_manage=[], force=False, return_type="message"):

  ## return_type = message (returns status messages), last_result (returns the result of the last command in the sql chain)
  ## If force= True, then if transaction manager fails to find tables, then it runs the SQL anyways
  ## You do not NEED to run SQL this way to rollback a transaction,
  ## but it automatically breaks up multiple statements in one SQL file into a series of spark.sql() commands

  stmts = sql_string.split(";")

  ## Save to class state
  self.raw_sql_statement = sql_string
  self.sql_statement_list = stmts

  success_tables = False

  try:
    self.begin_dynamic_transaction(tables_to_manage=tables_to_manage)

    success_tables = True

  except Exception as e:
    print(f"FAILED: failed to acquire tables with errors: {str(e)}")
  
  ## If succeeded or force = True, then run the SQL
  if success_tables or force:
    if success_tables == False and force == True:
      warnings.warn("WARNING: Failed to acquire tables but force flag = True, so SQL statement will run anyways")

    ## Run the Transaction Logic with Serverless Client
    try:
      
      print(f"TRANSACTION IN PROGRESS ...Running multi statement SQL transaction now\n")

      ## OPTION 1: return status message
      if return_type == "message":

        result_df = self.submit_multiple_sql_commands(sql_statements=sql_string)

      elif return_type == "last_result":
        
        result_df = self.submit_multiple_sql_commands_last_results(sql_statements=sql_string)

      else:
        result_df = None
        print("No run mode selected, select 'message' or 'last_results'")

      return result_df


      print(f"\n TRANSACTION SUCCEEDED: Multi Statement SQL Transaction Successfull! Updating Snapshot\n ")
      self.commit_transaction()
        
    except Exception as e:
      print(f"\n TRANSACTION FAILED to run all statements... ROLLING BACK \n")
      self.rollback_transaction()
      print(f"Rollback successful!")
      
      raise(e)

  else:

    raise(TransactionException(message="Failed to acquire tables and force=False, not running process.", errors="Failed to acquire tables and force=False, not running process."))
    

    