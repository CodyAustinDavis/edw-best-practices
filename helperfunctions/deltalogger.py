from pyspark.sql import SparkSession
import json
import re
from datetime import datetime

class DeltaLogger():

  ## User can pass in the fully qualified table name, use the spark session defaults, or pass in catalog and database overrides to the parameters. Pick one. 
  def __init__(self, logger_table_name = "delta_logger", logger_location=None, process_name = None, catalog=None, database=None):

    self.spark = SparkSession.builder.getOrCreate()

    self.session_process_name = process_name
    self.logger_location = logger_location
    self.logger_table_name = logger_table_name

    self.session_catalog = catalog
    self.session_database = database
    self.full_table_name = self.logger_table_name

    ## State for the active run
    self.active_run_id = None
    self.active_run_status = None
    self.active_run_start_ts = None
    self.active_run_end_ts = None 
    self.active_run_metadata = None

    ## 
    print(f"""Initializing Delta Logger {self.logger_table_name}\n Creating logger table if it does not exist...""")

    ## Add database and schema if provided
    if self.session_database is not None:
      self.full_table_name = self.session_database+ "." + self.full_table_name
    if self.session_catalog is not None:
      self.full_table_name = self.session_catalog + "." + self.full_table_name


    self.create_logger(logger_table_name=self.logger_table_name, logger_location=self.logger_location, catalog = self.session_catalog, database = self.session_database)


  ## Create a logger instance 
  def create_logger(self,logger_table_name=None,logger_location=None, catalog=None, database=None):

    full_table_name = logger_table_name

    ## Add database and schema if provided
    if database is not None:
      full_table_name = database+ "." + full_table_name
    if database is not None:
      full_table_name = catalog + "." + full_table_name


    ddl_sql = f"""CREATE TABLE IF NOT EXISTS {full_table_name} (
      run_id BIGINT GENERATED BY DEFAULT AS IDENTITY,
      process_name STRING NOT NULL,
      status STRING NOT NULL, -- RUNNING, FAIL, SUCCESS, STALE
      start_timestamp TIMESTAMP NOT NULL,
      end_timestamp TIMESTAMP,
      run_metadata STRING, -- String formatted like JSON
      update_timestamp TIMESTAMP
    )
    USING DELTA 
    """

    location_sql = f" LOCATION '{logger_location}' "
    partition_sql = " PARTITIONED BY (process_name)"

    if logger_location is not None:
      final_sql = ddl_sql + location_sql + partition_sql
    else: 
      final_sql = ddl_sql + partition_sql

    ## Try to initialize
    try: 

      self.spark.sql(final_sql)

      ## Optimize table as well on initilization
      self.spark.sql(f"OPTIMIZE {self.full_table_name} ZORDER BY (run_id, start_timestamp);")
      
      print(f"SUCCESS: Delta Logger Successfully Initialized:\n Table Name: {self.full_table_name}")
    
    except Exception as e: 
      msg = str(e)
      print(f"\n FAILED: DELTA LOGGER INIT FAILED with below error. Check you table name and scope: Table Name: {self.full_table_name}. \n  Error: \n {msg}")

      raise(e)
  

  ## Drop a logger instance
  def drop_logger(self, logger_table_name=None, catalog=None, database=None):
    
    full_table_name = logger_table_name

    ## Add database and schema if provided
    if database is not None:
      full_table_name = database+ "." + full_table_name
    if database is not None:
      full_table_name = catalog + "." + full_table_name

    try: 
      print(f"Dropping logger table: {full_table_name}...\n")
      self.spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
      print(f"SUCCESS: DROPPED LOGGER TABLE \n Table Name: {self.full_table_name}")
    
    except Exception as e: 
      msg = str(e)
      print(f"\n FAILED: DROP LOGGER FAILED with below error. Check you table name and scope: Table Name: {full_table_name}. \n  Error: \n {msg}")

      raise(e)


  ## Clear a logger table
  def truncate_logger(self, logger_table_name=None, catalog=None, database=None):
    
    full_table_name = logger_table_name

    ## Add database and schema if provided
    if database is not None:
      full_table_name = database+ "." + full_table_name
    if database is not None:
      full_table_name = catalog + "." + full_table_name

    try: 
      print(f"Truncating logger table: {full_table_name}...\n")
      self.spark.sql(f"TRUNCATE TABLE {full_table_name}")
      print(f"SUCCESS: TRUNCATED LOGGER TABLE \n Table Name: {self.full_table_name}")
    
    except Exception as e: 
      msg = str(e)
      print(f"\n FAILED: TRUNCATE LOGGER FAILED with below error. Check you table name and scope: Table Name: {full_table_name}. \n  Error: \n {msg}")

      raise(e)
    

  ## Create a new run and return the state for it
  def create_run(self, process_name = None, metadata: dict[str] = None, allow_concurrent_runs=False):

    ## Check for specific process override, otherwise look for session process

    if process_name is None:
      process_name = self.session_process_name
    
    ## Make start timestamp
    ts = datetime.now()
    start_ts = ts.strftime("%Y-%m-%d %H:%M:%S.%f")
    
    print(f"Starting and creating run for process: {process_name} \n Start Run at: {start_ts}")
    
    ## Parse and prepare any metadata
    status_key = "status"
    status_value = "CREATED"

    status_event_json = {status_key : status_value,
                        "event_timestamp": start_ts}


    default_metadata_struct = {"status_changes": [], "metadata": []}
    default_metadata_struct["status_changes"].append(status_event_json)
    default_metadata_struct["metadata"].append(metadata)

    pre_metadata = json.dumps(default_metadata_struct)


    try: 

      run_sql = f"""
        INSERT INTO {self.full_table_name} (process_name, start_timestamp, status, run_metadata, update_timestamp) VALUES ('{process_name}', '{start_ts}', 'RUNNING', '{pre_metadata}', now())
      """
      ## Create run
      self.spark.sql(run_sql)

      ## Optimize Process section
      self.spark.sql(f"""OPTIMIZE {self.full_table_name} WHERE process_name = '{process_name}' ZORDER BY (run_id, start_timestamp);""")
      
      

    except Exception as e:
      msg = str(e)
      print(f"\n FAILED: START LOGGER RUN with below error. Check you table name and scope: Table Name: {self.full_table_name}. \n  Error: \n {msg}")
      raise(e)
      
    try:

      ## Get run Metadata
      self.active_run_id, self.active_run_status, self.active_run_start_ts, self.active_run_end_ts, active_run_metadata = [i for i in self.spark.sql(f"""  
          SELECT
          run_id,
          status, 
          start_timestamp,
          end_timestamp,
          run_metadata
          FROM {self.full_table_name}
          WHERE 
          process_name = '{process_name}'
          AND start_timestamp = '{start_ts}'
          AND status = 'RUNNING'
          ORDER BY run_id DESC
          LIMIT 1
              """).collect()[0]]
      
      active_run_dict = json.loads(active_run_metadata)
      clean_run_metadata = re.sub("\'", "\"", active_run_metadata)
      active_run_dict = json.loads(clean_run_metadata)

      self.active_run_metadata = active_run_dict

      print(f"RUN CREATED with run_id = {self.active_run_id} for process = {process_name}")
    
      ### This automatically marks existing active runs as stale, thus only allowing one concurrent run per process
      if not allow_concurrent_runs: 
        print(f"WARNING: Cleaning up previous active runs for this process before creating new run because allow_concurrent_runs is FALSE")
        self.clean_up_stale_runs()


    except Exception as e:
      print(f"\n FAILED: Error getting new run metadata from: {self.full_table_name}. \n Error: \n {str(e)}")
      raise(e)

      
    return


  ### Resolve processs name between session and manually provided names
  def resolve_process_name(self, process_name=None):
    ## Resolve Process Name
    if process_name is None:
      process_name = self.session_process_name
    else: 
      process_name = process_name

    return process_name
  

  def resolve_run_id(self, run_id=None):
    ## Resolve Run Id
    if run_id is None:
      active_run_id = int(self.active_run_id)
    else: 
      active_run_id = int(run_id)

    return active_run_id



  ## Get status for run id or active run if
  def get_status_for_run_id(self, run_id=None, process_name=None):

    ## Resolve Process Name
    process_name = self.resolve_process_name(process_name = process_name)
    ## Resolve Run Id
    run_id = self.resolve_run_id(run_id = run_id)

    try:

      new_active_run_status = int(self.spark.sql(f"""
                                        SELECT MAX(status) FROM {self.full_table_name} 
                                        WHERE process_name = '{process_name}'
                                        AND run_id = {run_id}
                                        """).collect()[0][0])
    
    except Exception as e:
      print(f"FAIL: Unable to get metadata for run id {run_id} in process: {process_name} with error: \n {str(e)}")
      raise(e)
    
    return new_active_run_status
  

  ## Get start time for a given run id
  ## Defaults to getting the active run in the process
  def get_start_time_for_run_id(self, run_id=None, process_name=None):

    ## Resolve Process Name
    process_name = self.resolve_process_name(process_name = process_name)
    ## Resolve Run Id
    run_id = self.resolve_run_id(run_id = run_id)

    try:

      active_run_start_time = self.spark.sql(f"""
                                        SELECT MAX(start_timestamp) FROM {self.full_table_name} 
                                        WHERE process_name = '{process_name}'
                                        AND run_id = {run_id}
                                        """).collect()[0][0]
    
    except Exception as e:
      print(f"FAIL: Unable to get start timestamp for run id {run_id} in process: {process_name} with error: \n {str(e)}")
      raise(e)
    
    return active_run_start_time
  

  ## Get start time for a given run id
  ## Defaults to getting the active run in the process
  def get_end_time_for_run_id(self, run_id=None, process_name=None):

    ## Resolve Process Name
    process_name = self.resolve_process_name(process_name = process_name)
    ## Resolve Run Id
    run_id = self.resolve_run_id(run_id = run_id)

    try:

      active_run_end_time = self.spark.sql(f"""
                                        SELECT MAX(end_timestamp) FROM {self.full_table_name} 
                                        WHERE process_name = '{process_name}'
                                        AND run_id = {run_id}
                                        """).collect()[0][0]
    
    except Exception as e:
      print(f"FAIL: Unable to get start timestamp for run id {run_id} in process: {process_name} with error: \n {str(e)}")
      raise(e)
    
    return active_run_end_time
  


  ## Get metadata for run id for a given process or active run id
  def get_metadata_for_run_id(self, run_id=None, process_name=None):

    ## Resolve Process Name
    process_name = self.resolve_process_name(process_name = process_name)
    ## Resolve Run Id
    run_id = self.resolve_run_id(run_id = run_id)


    try:
    
      
      new_active_run_metadata = json.loads(self.spark.sql(f"""
                                        SELECT MAX(run_metadata) FROM {self.full_table_name} 
                                        WHERE process_name = '{process_name}'
                                        AND run_id = {run_id}
                                        """).collect()[0][0])
    
    except Exception as e:
      print(f"UPDATE: No run metadata found for run id: {run_id} on process: {process_name}. Create new dataset")
      new_active_run_metadata = {"status_changes": [], "metadata": []}

    return new_active_run_metadata


  ## Get most recent run id for a given process
  def get_most_recent_run_id(self, process_name=None, status=None):

    ## Resolve Process Name
    process_name = self.resolve_process_name(process_name = process_name)

    sql_query = f"""SELECT MAX(run_id) FROM {self.full_table_name} WHERE process_name = '{process_name}' """

    ## Resolve optional status filter
    if status is None:
      pass
    else: 
      if status not in ["RUNNING", "SUCCESS", "FAILED", "STALE"]:
        raise(ValueError(f"{status} not in allowed values: RUNNING, SUCCESS, FAILED, STALE"))
      
      else: 
        sql_query += f" AND status = '{status}'"

    try: 

      try:
        new_active_run_id = int(self.spark.sql(sql_query).collect()[0][0])

      except:
        new_active_run_id = -1

    except Exception as e:
      print(f"FAIL: Unable to get status of most recent run id for process {process_name} with error: \n {str(e)}")
      raise(e)

    return new_active_run_id
  


  ## Functions important for watermarking
  def get_most_recent_success_run_start_time(self, process_name=None):
    process_name = self.resolve_process_name(process_name = process_name)

    try:
      most_recent_run_id = self.get_most_recent_run_id(process_name=process_name, status="SUCCESS")

      if most_recent_run_id == -1:
        return "1900-01-01 00:00:00.000"
      
      else: 
        latest_success_start_timetamp = self.get_start_time_for_run_id(run_id=most_recent_run_id, process_name=process_name)

    except Exception as e:
      print(f"FAIL: Unable to get success start time of most recent run for process: {process_name} with error: \n {str(e)}")
      raise(e)  
    
    return latest_success_start_timetamp
  

  def get_most_recent_success_run_end_time(self, process_name=None):
    process_name = self.resolve_process_name(process_name = process_name)

    try:
      most_recent_run_id = self.get_most_recent_run_id(process_name=process_name, status="SUCCESS")

      if most_recent_run_id == -1:
        return "1900-01-01 00:00:00.000"
      
      else:
        latest_success_end_timetamp = self.get_end_time_for_run_id(run_id=most_recent_run_id, process_name=process_name)

    except Exception as e:
      print(f"FAIL: Unable to get success end time of most recent run for process: {process_name} with error: \n {str(e)}")
      raise(e)  
    
    return latest_success_end_timetamp
  

  def get_most_recent_fail_run_start_time(self, process_name=None):
    process_name = self.resolve_process_name(process_name = process_name)

    try:
      most_recent_run_id = self.get_most_recent_run_id(process_name=process_name, status="FAIL")
      
      if most_recent_run_id == -1:
        return "1900-01-01 00:00:00.000"
      
      else:
        latest_success_start_timetamp = self.get_start_time_for_run_id(run_id=most_recent_run_id, process_name=process_name)

    except Exception as e:
      print(f"FAIL: Unable to get failed start time of most recent run for process: {process_name} with error: \n {str(e)}")
      raise(e)  
    
    return latest_success_start_timetamp
  

  def get_most_recent_fail_run_end_time(self, process_name=None):
    process_name = self.resolve_process_name(process_name = process_name)

    try:
      most_recent_run_id = self.get_most_recent_run_id(process_name=process_name, status="FAIL")

      if most_recent_run_id == -1:
        return "1900-01-01 00:00:00.000"
      
      else:
        latest_success_end_timetamp = self.get_end_time_for_run_id(run_id=most_recent_run_id, process_name=process_name)

    except Exception as e:
      print(f"FAIL: Unable to get failed end time of most recent run for process: {process_name} with error: \n {str(e)}")
      raise(e)   
    
    return latest_success_end_timetamp


  #### Stateful functions to get data for most recent run in a process
  ## This is DIFFERENT than whatever the active run id is
  ## Get status of most recent run
  def get_most_recent_run_status(self, process_name=None):

    ## Resolve Process Name
    process_name = self.resolve_process_name(process_name = process_name)

    try: 
      most_recent_run_id = self.get_most_recent_run_id(process_name=process_name)
      most_recent_run_status = self.get_status_for_run_id(run_id=most_recent_run_id, process_name = process_name)

    except Exception as e: 
      print(f"FAIL: Unable to get status of most recent run for process: {process_name} with error: \n {str(e)}")
      raise(e)

    return most_recent_run_status


  ## Get status of most recent run
  def get_most_recent_run_metadata(self, process_name=None):

    most_recent_run_metadata = None

    ## Resolve Process Name
    process_name = self.resolve_process_name(process_name = process_name)

    try: 
      most_recent_run_id = self.get_most_recent_run_id(process_name=process_name)
      most_recent_run_metadata = self.get_metadata_for_run_id(run_id=most_recent_run_id, process_name = process_name)

    except Exception as e: 
      print(f"FAIL: Unable to get status of most recent run metadata for process: {process_name} with error: \n {str(e)}")
      raise(e)

    return most_recent_run_metadata
  
  
  ## INTERNAL ONLY, this can update anything. We dont need to expose this
  def _update_run_id(self, process_name = None, run_id = None, status=None, start_timestamp=None, end_timestamp=None, run_metadata=None):

    process_name = self.resolve_process_name(process_name)
    run_id = self.resolve_run_id(run_id)

    current_run_metadata = self.get_metadata_for_run_id(process_name=process_name, run_id = run_id)

    ## Root SQL

    base_sql = f"""UPDATE {self.full_table_name} AS tbl 
                  SET  
    """

    if status is not None:
      base_sql += f" status = '{status}', "

          ## Make start timestamp
      ts = datetime.now()
      status_ts = ts.strftime("%Y-%m-%d %H:%M:%S.%f")

      ## Parse and prepare any metadata
      status_key = "status"
      status_value = status

      status_event_json = {status_key : status_value,
                        "event_timestamp": status_ts}
      
      current_run_metadata["status_changes"].append(status_event_json)


    if start_timestamp is not None: 
      base_sql += f" start_timestamp = '{start_timestamp}', "

    if end_timestamp is not None: 
      base_sql += f" end_timestamp = '{end_timestamp}', "

    if run_metadata is not None: 
      current_run_metadata["metadata"].append(run_metadata)
    
    
    if status is not None or run_metadata is not None:
      prepped_run_metadata = json.dumps(current_run_metadata)
      base_sql += f" run_metadata = '{prepped_run_metadata}', "


    base_sql += f""" update_timestamp = now()  
                WHERE process_name = '{process_name}'
                AND run_id = {run_id}
                ;
                """

    try: 

      print(f"Updating RUN {run_id} for process {process_name}")
      self.spark.sql(base_sql)

    except Exception as e:
      print(f"FAIL: Unable to update run_id {run_id} for process: {process_name} with error: \n {str(e)}")
      raise(e)

    return
  
  
  ## Clean up any older run ids for a given process that are still running but have no end timestamp
  ## TO DO: Make this set an identifier in run metadata
  def clean_up_stale_runs(self, process_name=None):

    ## Resolve Process Name
    process_name = self.resolve_process_name(process_name = process_name)

    ## Resolve Run Id
    active_run_id = self.get_most_recent_run_id(process_name=process_name, status="RUNNING")

    try:
      self.spark.sql(f"""
                     UPDATE {self.full_table_name} AS tbl
                      SET 
                      status = 'STALE',
                      end_timestamp = now(),
                      update_timestamp = now()
                     WHERE process_name = '{process_name}'
                     AND run_id < {active_run_id}
                     AND (status NOT IN ('FAIL', 'SUCCESS', 'STALE') OR end_timestamp IS NULL);
                     """)
      
      self.spark.sql(f"OPTIMIZE {self.full_table_name} ZORDER BY (end_timestamp, start_timestamp, run_id);")

    except Exception as e:
      print(f"FAIL to clean stale runs with error: {str(e)}")

    ## Now clean up stale runs
    """
    1. With active run id / status - update all runs with run_id < active_run_id AND status != 'SUCCESS'/'FAILED'
    2. Set all flagged run ids with STATUS = 'STALE', add key in metadata to add that it was flagged as stale because of new concurrent run of same process
    """
    return
  

  ## Finally, complete the run!
  def complete_run(self, process_name=None, run_id=None, metadata=None):
    status = "SUCCESS"
    process_name = self.resolve_process_name(process_name)
    run_id = self.resolve_run_id(run_id)

    ## Make start timestamp
    ts = datetime.now()
    end_ts = ts.strftime("%Y-%m-%d %H:%M:%S.%f")

    try:

      self._update_run_id(process_name=process_name, run_id=run_id, status=status, end_timestamp=end_ts, run_metadata=metadata)

      self.active_run_end_ts = None
      self.active_run_id = None
      self.active_run_metadata = None
      self.active_run_start_ts = None
      self.active_run_status = None

      ## Optimize Process section
      self.spark.sql(f"""OPTIMIZE {self.full_table_name} WHERE process_name = '{process_name}' ZORDER BY (run_id, start_timestamp);""")
      

      print(f"COMPLETED RUN {run_id} for process {process_name} at {end_ts}!")

    except Exception as e:
      print(f"FAIL to mark run complete for run_id {run_id} in process {process_name} with error: {str(e)}")
      raise(e)
    
    return
  

  def fail_run(self, process_name=None, run_id=None, metadata=None):

    status = "FAIL"
    process_name = self.resolve_process_name(process_name)
    run_id = self.resolve_run_id(run_id)

    ## Make start timestamp
    ts = datetime.now()
    end_ts = ts.strftime("%Y-%m-%d %H:%M:%S.%f")

    try:

      self._update_run_id(process_name=process_name, run_id=run_id, status=status, end_timestamp=end_ts, run_metadata=metadata)

      self.active_run_end_ts = None
      self.active_run_id = None
      self.active_run_metadata = None
      self.active_run_start_ts = None
      self.active_run_status = None

      ## Optimize Process section
      self.spark.sql(f"""OPTIMIZE {self.full_table_name} WHERE process_name = '{process_name}' ZORDER BY (run_id, start_timestamp);""")
      

      print(f"FAILED RUN {run_id} for process {process_name} at {end_ts}!")

    except Exception as e:
      print(f"FAIL to mark run FAIL for run_id {run_id} in process {process_name} with error: {str(e)}")
      raise(e)

    return
  

  ## Add logging events to active runs
  ## Commit immediately = True will update the delta table record synchronously, False will store update in the instance and commit on complete/fail
  ## Options for level INFO/WARN/DEBUG/CRITICAL
  def log_run_info(self, log_level:str ="INFO", msg:str = None, process_name=None, run_id=None):

    if log_level not in ["DEBUG", "INFO", "WARN", "CRITICAL"]:
      raise(ValueError("log_level must be one of the following values: DEBUG/INFO/WARN/CRITICAL"))
    

    process_name = self.resolve_process_name(process_name)
    run_id = self.resolve_run_id(run_id)

    ts = datetime.now()
    log_ts = ts.strftime("%Y-%m-%d %H:%M:%S.%f")


    current_run_metadata = self.get_metadata_for_run_id(run_id)

    run_metadata = {"log_level": log_level, "log_data": {"event_ts": log_ts, "msg": msg}}
    

    try: 

      if msg is not None:

        self._update_run_id(process_name=process_name, run_id=run_id, run_metadata=run_metadata)
        print(f"{log_level} - {log_ts} for {run_id} in process {process_name}. MSG: {run_metadata}")
        
      else:
        print("No msg to log for run. Skipping. ")



    except Exception as e:
      print(f"FAILED to log event for run_id {run_id} and process_name {process_name} with error: {str(e)}")
      raise(e)
    
    return