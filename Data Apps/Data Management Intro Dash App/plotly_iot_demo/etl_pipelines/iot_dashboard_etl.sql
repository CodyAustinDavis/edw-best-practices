TRUNCATE TABLE main.plotly_iot_dashboard.bronze_users;
TRUNCATE TABLE main.plotly_iot_dashboard.bronze_sensors;


-- DBTITLE 1,Incrementally Ingest Source Data from Raw Files
COPY INTO main.plotly_iot_dashboard.bronze_sensors
FROM (SELECT 
id::bigint AS Id,
device_id::integer AS device_id,
user_id::integer AS user_id,
calories_burnt::decimal(10,2) AS calories_burnt, 
miles_walked::decimal(10,2) AS miles_walked, 
num_steps::decimal(10,2) AS num_steps, 
timestamp::timestamp AS timestamp,
value AS value
FROM "/databricks-datasets/iot-stream/data-device/")
FILEFORMAT = json
COPY_OPTIONS('force'='true') --option to be incremental or always load all files
;


-- DBTITLE 1,Perform Upserts - Device Data
MERGE INTO main.plotly_iot_dashboard.silver_sensors AS target
USING (SELECT Id::integer,
              device_id::integer,
              user_id::integer,
              calories_burnt::decimal,
              miles_walked::decimal,
              num_steps::decimal,
              timestamp::timestamp,
              value::string
              FROM main.plotly_iot_dashboard.bronze_sensors) AS source
ON source.Id = target.Id
AND source.user_id = target.user_id
AND source.device_id = target.device_id
WHEN MATCHED THEN UPDATE SET 
  target.calories_burnt = source.calories_burnt,
  target.miles_walked = source.miles_walked,
  target.num_steps = source.num_steps,
  target.timestamp = source.timestamp
WHEN NOT MATCHED THEN INSERT *;

--Truncate bronze batch once successfully loaded
TRUNCATE TABLE main.plotly_iot_dashboard.bronze_sensors;

-- COMMAND ----------

-- DBTITLE 1,Table Optimizations
OPTIMIZE main.plotly_iot_dashboard.silver_sensors ZORDER BY (user_id, device_id, timestamp);

-- COMMAND ----------

-- DBTITLE 1,Incrementally Ingest Raw User Data
COPY INTO main.plotly_iot_dashboard.bronze_users
FROM (SELECT 
userid::bigint AS user_id,
gender AS gender,
age::integer AS age,
height::decimal(10,2) AS height, 
weight::decimal(10,2) AS weight,
smoker AS smoker,
familyhistory AS familyhistory,
cholestlevs AS cholestlevs,
bp AS bp,
risk::decimal(10,2) AS risk,
current_timestamp() AS update_timestamp
FROM "/databricks-datasets/iot-stream/data-user/")
FILEFORMAT = CSV
FORMAT_OPTIONS('header'='true')
COPY_OPTIONS('force'='true') --option to be incremental or always load all files
;


MERGE INTO main.plotly_iot_dashboard.silver_users AS target
USING (SELECT 
      user_id::int,
      gender::string,
      age::int,
      height::decimal, 
      weight::decimal,
      smoker,
      familyhistory,
      cholestlevs,
      bp,
      risk,
      update_timestamp
      FROM main.plotly_iot_dashboard.bronze_users) AS source
ON source.user_id = target.user_id
WHEN MATCHED THEN UPDATE SET 
  target.gender = source.gender,
      target.age = source.age,
      target.height = source.height, 
      target.weight = source.weight,
      target.smoker = source.smoker,
      target.familyhistory = source.familyhistory,
      target.cholestlevs = source.cholestlevs,
      target.bp = source.bp,
      target.risk = source.risk,
      target.update_timestamp = source.update_timestamp
WHEN NOT MATCHED THEN INSERT *;

--Truncate bronze batch once successfully loaded
TRUNCATE TABLE main.plotly_iot_dashboard.bronze_users;




-- Create Gold Table and Read via Reflection

CREATE OR REPLACE TABLE main.plotly_iot_dashboard.gold_sensors
AS
(SELECT timestamp,
-- Number of Steps
(avg(`num_steps`) OVER (
        ORDER BY timestamp
        ROWS BETWEEN
          15 PRECEDING AND
          CURRENT ROW
      )) ::float AS SmoothedNumSteps30SecondMA, -- 30 second moving average
     
(avg(`num_steps`) OVER (
        ORDER BY timestamp
        ROWS BETWEEN
          60 PRECEDING AND
          CURRENT ROW
      ))::float AS SmoothedNumSteps120SecondMA,--120 second moving average,
-- Calories Burnt
(avg(`calories_burnt`) OVER (
        ORDER BY timestamp
        ROWS BETWEEN
          15 PRECEDING AND
          CURRENT ROW
      )) ::float AS SmoothedCaloriesBurnt30SecondMA, -- 30 second moving average
     
(avg(`calories_burnt`) OVER (
        ORDER BY timestamp
        ROWS BETWEEN
          60 PRECEDING AND
          CURRENT ROW
      ))::float AS SmoothedCaloriesBurnt120SecondMA --120 second moving average
FROM main.plotly_iot_dashboard.silver_sensors
)