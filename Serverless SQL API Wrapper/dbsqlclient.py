import requests
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import pyarrow
from urllib.parse import urljoin, urlencode
import json
from pyspark.sql.types import *
from pyspark.dbutils import DBUtils
import IPython



      

"""

## Library to emulate the spark.sql functionality to allow for drop in replacement of spark.sql() methods with some additional functionality such as:
# 1. Get Status of running query
# 2. Stop Running Query
# 3. Submit async query

## ! This class tracks state for 1 query at a time, just like spark.sql, so if you want to manage and submit concurrent queries, initialize a separate object for it. 

## WARNING: The results of the query are NOT distributed and are very similar to calling spark.sql().display(). The best design pattern here for large computations is to write INSERTS/MERGEs into Delta tables (either permanent or temporary) and then call the original spark.sql to read in the results in a distributed fashion. 


"""

class ServerlessClient():

  def __init__(self, warehouse_id: str, token: str, host_name: str = None, verbose : bool = False):

    ## Assume running in a spark environment, use same session as caller
    ## Defaults to same workspace that the client is in, but can manually override by passing in host_name 

    self.spark = SparkSession.getActiveSession()

    self.verbose = verbose

    self.dbutils = None
    if self.spark.conf.get("spark.databricks.service.client.enabled") == "true":
      self.dbutils = DBUtils(self.spark)
    else:
      self.dbutils = IPython.get_ipython().user_ns["dbutils"]

    
    if host_name is not None:
      self.host_name = host_name
    else:
      self.host_name = json.loads(self.dbutils.entry_point.getDbutils().notebook().getContext().toJson()).get("tags").get("browserHostName")

    self.warehouse_id = warehouse_id
    self.token = token

    self.uri = f"https://{self.host_name}/api/2.0/sql/statements"

    self.headers_auth = {"Authorization":f"Bearer {self.token}"}

    print(f"Initialized Serverless Client for warehouse: {self.warehouse_id} on workspace: {self.host_name}")

    ## Track a statement and store the active statement id
    self.active_statement_id = None
    self.active_sql_statement = None
    self.statement_status = None
    self.statement_return_payload = None
    self.multi_statement_result_state = None


  ## Not used, not necessary
  @staticmethod
  def convert_to_struct_from_json_array(json_schema_response) -> StructType:

    temp_struct = {"fields": [], "type":"struct"}

    for i, j in enumerate(result_schema):

      ## convert data types

      ## If ARRAY type:
      if j.get("type_text").lower().startswith("array"):

        data_type, element_type = [i for i in re.sub("[<>]", " ", j.get("type_text").lower()).split(" ") if len(i) >0 ]

        new_field = {"metadata": {}, "name": j.get("name"), "nullable": True, "type": {"containsNull":True,"elementType":element_type,"type":data_type}}

      ### TO DO: Deal with other nested data types + recursive nesting of array types\
        
      ## For non-nested fields
      else:
        new_field = {"metadata": {}, "name": j.get("name"), "nullable": True, "type": j.get("type_text").lower()}
      temp_struct["fields"].append(new_field)

    clean_struct = StructType.fromJson(temp_struct)

    return clean_struct


  @staticmethod
  def create_df_from_json_array(result_data, result_schema) -> DataFrame:
    ## input, result data (JSONARRAY, raw result schema)

    field_names = [i.get("name") for i in result_schema]
    temp_df = self.spark.createDataFrame(result_data).toDF(*field_names)

    ## Cast the associated column types

    cast_expr = []
    ## Build SQL Expression
    for i,j  in enumerate(temp_df.columns):
      cast_d_type = result_schema[i].get("type_text")
      #print(f"{j} --> {cast_d_type}")

      ## TO DO:  Deal with STRUCT types

      if cast_d_type.startswith("ARRAY"):

        ep = f"from_json({j},  '{cast_d_type}') AS {j}"

      else:
        ep = f"CAST({j} AS {cast_d_type}) AS {j}"

      cast_expr.append(ep)
      

    clean_df = temp_df.selectExpr(*cast_expr)

    return clean_df


  def prepare_final_df_from_json_array(self, endp_resp) -> DataFrame:
    ## Parse results into data frame function

    try: 
      result_schema = endp_resp.get("manifest").get("schema").get("columns")
      result_format = endp_resp.get("manifest").get("format")
      result_chunk_count = endp_resp.get("manifest").get("total_chunk_count")
      result_total_row_count = endp_resp.get("manifest").get("total_row_count")
      result_is_partial = endp_resp.get("manifest").get("truncated")
      result_chunk_array = endp_resp.get("manifest").get("chunks")

      if result_format == "JSON_ARRAY":
          
        result_data = endp_resp.get("result").get("data_array")

        ## Not used here
        #result_clean_schema = self.convert_to_struct_from_api_resp(result_schema)

        clean_df = self.create_df_from_json_array(result_data, result_schema)

        return clean_df
      
      else: 
        
        if self.verbose == True:
          print(f"Response is not JSON_ARRAY... and is: {result_format} instead, use different result parsing function")

        return
      
    except Exception as e:
      if self.verbose == True:
        print(f"Failed to build and return df.... here is the response: {endp_resp}")

      raise(e)



  def poll_for_statement_status(self, max_wait_time = 30):

    statement_status = self.statement_status

    num_retries = 0
    max_wait_time = max_wait_time

    wait_time_in_seconds = 1

    ## Check current status first, if not pending, then just return this
    check_status_resp = requests.get(self.uri + "/" + self.active_statement_id, headers=self.headers_auth).json()
    statement_status = check_status_resp.get("status").get("state")
        
    while statement_status in ["RUNNING", "PENDING"]:

      check_status_resp = requests.get(uri + "/" + self.active_statement_id, headers=self.headers_auth).json()
      statement_status = check_status_resp.get("status").get("state")

      ## update internal status of query

      self.statement_status = statement_status

      if self.verbose == True:
        print(statement_status)

      if statement_status in ["SUCCEEDED", "FAILED", "CLOSED"]:
          ## Gets status of the current statement id in queue
        return check_status_resp  


      num_retries += 1
      wait_time_in_seconds = 2**(num_retries-1) ## Wait longer up to max wait time for failed operations

      if wait_time_in_seconds > max_wait_time:
          wait_time_in_seconds = max_wait_time

      if self.verbose == True:
        print(f"Waiting {wait_time_in_seconds} seconds before next poll attempt on {statement_id}...")

      time.sleep(wait_time_in_seconds)





  def stop_sql_async(self):

    try: 

      cancel_resp = requests.post(self.uri + f"/{self.active_statement_id}" + "/cancel", hedaers=self.headers_auth)

      if self.verbose == True:
        print(f"Command CANCELLED {self.active_statement_id}: \n {self.active_sql_statement} \n")

      return cancel_resp
    
    except Exception as e:
      if self.verbose == True:
        print(f"Canellation Failed with error: {str(e)}")
      raise(e)



  ## Process parallel chunks in parallel Needs to process the 0th chunk differently
  def build_dataframe_from_chunks(self, response, return_type = "spark", limit=None):

      ## spark or pandas return type
      self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

      processing_statement_id = response["statement_id"]
      chunks = response["manifest"]["chunks"]
      tables = []

      if self.verbose == True:
        print("{} chunks(s) in result set".format(len(chunks)))

      for idx, chunk_info in enumerate(chunks):


          if chunk_info["chunk_index"] == 0:

            chunk_0_external_links = [i.get("external_link") for i in response['result']['external_links']][0]

            # NOTE: do _NOT_ send the authorization header to external urls
            raw_response = requests.get(chunk_0_external_links, auth=None, headers=None)
            assert raw_response.status_code == 200

            arrow_table = pyarrow.ipc.open_stream(raw_response.content).read_all()
            tables.append(arrow_table)

            if self.verbose == True:
              print("chunk {} received".format(idx))

          else:
            
            ## Process 1 to N chunks by paging through internal links
            ## Next to get next chunk from previous
            stmt_url = self.uri + "/" +  processing_statement_id + "/"

            row_offset_param = urlencode({'row_offset': chunk_info["row_offset"]})
            
            if self.verbose == True:
              print(stmt_url)

            resolve_external_link_url = stmt_url + "result/chunks/{}?{}".format(
                chunk_info["chunk_index"], row_offset_param)

            if self.verbose == True:
              print(resolve_external_link_url)

            response = requests.get(resolve_external_link_url, headers=self.headers_auth)
            
            if self.verbose == True:
              print(response.json())

            assert response.status_code == 200

            external_url = response.json()["external_links"][0]["external_link"]
            # NOTE: do _NOT_ send the authorization header to external urls
            raw_response = requests.get(external_url, auth=None, headers=None)
            assert raw_response.status_code == 200

            arrow_table = pyarrow.ipc.open_stream(raw_response.content).read_all()
            tables.append(arrow_table)

            if self.verbose == True:
              print("chunk {} received".format(idx))

      full_table = pyarrow.concat_tables(tables).to_pandas()

      if return_type == "pandas":
        return full_table

      else: 
        final_spark_df = self.spark.createDataFrame(full_table)
        return final_spark_df


  ## for large results -- compiles external links together 
  def submit_sql_async_external_links(self, sql_statement: str, return_type = "dataframe", result_format = "ARROW_STREAM"):

    #EXTERNAL_LINKS + ARROW_STREAM
    ### Can return "dataframe" or the resulting payload with "message" or just status with "status"
    ## Do not use auth header in this call since links are pre-signed

    self.active_sql_statement = sql_statement

    request_string = {
    "statement": self.active_sql_statement,
    "warehouse_id": self.warehouse_id,
    "format": result_format,
    "disposition": "EXTERNAL_LINKS"
    }

    ## Convert dict to json
    request_payload = json.dumps(request_string)

    #### Get Query History Results from API
    endp_resp = requests.post(self.uri, data=request_payload, headers=self.headers_auth).json()

    self.active_statement_id = endp_resp.get("statement_id")
    self.active_sql_statement = sql_statement
    self.statement_status = endp_resp.get("status").get("state")
    self.statement_return_payload = endp_resp


    ## Check for status, poll until SUCCEEDED or FAILED or CLOSED

    if self.statement_status in ["SUCCEEDED", "CLOSED"]:
      final_response = self.statement_return_payload

    else:
      final_response = self.poll_for_statement_status()
      self.statement_return_payload = final_response
      self.statement_status = final_response.get("status").get("state")
    
    self.statement_return_payload = final_response

    if return_type == "message":
      return self.statement_return_payload
    
    elif return_type == "dataframe":
      
      results_df = self.build_dataframe_from_chunks(self.statement_return_payload)
      return results_df

    elif return_type == "status":
      return self.statement_status
  


  ## submit async query (the additional methods couple the ability to cancel and get the status of the async query)
  def submit_sql_sync_in_line(self, sql_statement: str, return_type = "dataframe"):

    ### Can return "dataframe" or the resulting payload with "message" or just status with "status"
    # INLINE + JSON_ARRAY

    self.active_sql_statement = sql_statement

    request_string = {
    "statement": self.active_sql_statement,
    "warehouse_id": self.warehouse_id,
    "format": "JSON_ARRAY",
    "disposition": "INLINE"
    }

    ## Convert dict to json
    request_payload = json.dumps(request_string)

    #### Get Query History Results from API
    endp_resp = requests.post(self.uri, data=request_payload, headers=self.headers_auth).json()

    self.active_statement_id = endp_resp.get("statement_id")
    self.active_sql_statement = sql_statement
    self.statement_status = endp_resp.get("status").get("state")
    self.statement_return_payload = endp_resp


    ## Check for status, poll until SUCCEEDED or FAILED or CLOSED

    if self.statement_status in ["SUCCEEDED", "CLOSED"]:
      final_response = self.statement_return_payload

    else:
      final_response = self.poll_for_statement_status()
      self.statement_return_payload = final_response
      self.statement_status = final_response.get("status").get("state")

    ## Parse response
    if return_type == "dataframe":
      final_df = self.prepare_final_df_from_json_array(endp_resp=final_response)
      return final_df
    
    elif return_type == "message":
      return self.statement_return_payload
    
    elif return_type == "status":
      return self.statement_status
    
    


  ### These are the user-facing wrappers that abstract away the need to deal with async vs sync and polling

  ## This method wraps all the async functions above into a synchronous call to mimic spark.sql()
  def sql(self, sql_statement: str, process_mode = "default", return_type = "dataframe") -> DataFrame:
    
    """
    process_modes
    default - will automatically try single threaded synchrounous response, and if results are too big it will chunk it
    inline - will only use the inline sync command
    parallel - will only use the async with external links command

    """

    if process_mode == "default":

      ## Every time this is called, it abandons the previous statement id and replaces it with a new one
      try:
        ## Try small version first - if results are too big it will fail automatically
        ## Since we cant generically anticipate result size, trying this first is the only way unless users manually uses one of the underlying functions if they know
        final_df = self.submit_sql_sync_in_line(sql_statement, return_type = return_type)
        return final_df

      except:
        if self.verbose == True:
          print("Result too large to inline... moving to external links...")

        final_df = self.submit_sql_async_external_links(sql_statement, return_type = return_type)
        return final_df
      
    elif process_mode == "inline":
      final_df = self.submit_sql_sync_in_line(sql_statement, return_type = return_type)
      return final_df
    
    elif process_mode =="parallel":
      final_df = self.submit_sql_async_external_links(sql_statement, return_type = return_type)
      return final_df  



   ## This method wraps all the async functions above into a synchronous call to mimic spark.sql()
  def sql_no_results(self, sql_statement: str, process_mode = "default"):
    
    return_msg = self.sql(sql_statement = sql_statement, process_mode = process_mode, return_type = "status")

    ## Every time this is called, it abandons the previous statement id and replaces it with a new one
    return  return_msg



  ## Submits chains of SQL commands with ; delimeter and tracks status of each command, results status of all commands
  def submit_multiple_sql_commands(self, sql_statements: str, process_mode = "default", return_type = "message", full_results=False): 

    command_array = sql_statements.split(";")
    failed_queries = 0
    command_chain_dict = {"ALL_SUCCESS": None, "STATEMENTS": []}

    self.multi_statement_result_state = command_chain_dict

    for i, query in enumerate(command_array):

      return_msg = self.sql(sql_statement = query, process_mode = process_mode, return_type = "message")
      command_state = return_msg.get("status").get("state")

      if command_state != "SUCCEEDED":
        failed_queries += 1
        command_chain_dict["ALL_SUCCESS"] = False

      if full_results == True:
        command_chain_dict["STATEMENTS"].append({"query": query, "status": command_state, "response": return_msg})
      else:
        command_chain_dict["STATEMENTS"].append({"query": query, "status": command_state})

    if failed_queries == 0:
      command_chain_dict["ALL_SUCCESS"] = True

    self.multi_statement_result_state = command_chain_dict

    return command_chain_dict
  

  ## Submit SQL commands with NO results, just the API messages / success or failure
  def submit_multiple_sql_commands_last_results(self, sql_statements: str, process_mode = "default"): 

    ## This function could be better improved for further tracking of final results, this is really not a great design pattern in production 
    ## Because if a query fails somewhere in the chain and we still return the results of the last query, this currently doesnt track the failures and return them
    ## What we will do is if ANY queries fail, raise error

    command_array = sql_statements.split(";")
    failed_queries = 0
    command_chain_dict = {"ALL_SUCCESS": None, "STATEMENTS": []}

    ## If a multi statement command fails, we can access the state of the commands
    self.multi_statement_result_state = command_chain_dict

    ## Check for last record in dataframe, if last record, return results instread of the message
    for i, query in enumerate(command_array):

      try:
        if i == (len(command_array) - 1):
          return_df = self.sql(sql_statement = query, process_mode = process_mode, return_type = "dataframe")

          ## Get the last query state results as well
          command_state = self.statement_status
          return_msg = self.statement_return_payload
          
          if command_state != "SUCCEEDED":
            failed_queries += 1
            command_chain_dict["ALL_SUCCESS"] = False
          
          ## Save checkpoint
          command_chain_dict["STATEMENTS"].append({"query": query, "status": command_state, "response": return_msg})
          self.multi_statement_result_state = command_chain_dict

          if self.verbose == True:
            print(f"Query {i} -> {command_state} -> {query}")

          ## When processing last query right before we return results, save status of full query DAG
          if failed_queries == 0:
            command_chain_dict["ALL_SUCCESS"] = True
          return return_df

        else:
          return_msg = self.sql(sql_statement = query, process_mode = process_mode, return_type = "message")
          command_state = return_msg.get("status").get("state")

          if self.verbose == True:
            print(f"Query {i} -> {command_state} -> {query}")

          if command_state != "SUCCEEDED":
            failed_queries += 1
            command_chain_dict["ALL_SUCCESS"] = False

          ## Save checkpoint
          command_chain_dict["STATEMENTS"].append({"query": query, "status": command_state, "response": return_msg})
          self.multi_statement_result_state = command_chain_dict

      except Exception as e:

        ## Save Status of current command chain dict
        self.multi_statement_result_state = command_chain_dict
        print(f"One of the queries in the chain failed, cannot safely return last result set... {command_chain_dict}")
        raise(e)
