// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Utilites to Import

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Ingestion Class

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC import org.apache.spark.sql.functions.{unix_timestamp,input_file_name,col};
// MAGIC import org.apache.spark.sql.streaming.Trigger;
// MAGIC import org.apache.spark.sql.streaming.{StreamingQuery};
// MAGIC import org.apache.spark.sql.catalyst.util.UnknownFieldException;
// MAGIC import org.apache.spark.sql.DataFrame;
// MAGIC 
// MAGIC /*
// MAGIC   Write data from Autloader stream to Delta Lake
// MAGIC */
// MAGIC 
// MAGIC class DeltaLoader(
// MAGIC     fileType: String,
// MAGIC     checkpoint_path: String,
// MAGIC     schema_location: String,
// MAGIC     landingzone_path : String,
// MAGIC     write_path: String,
// MAGIC     start_schema: String = ""
// MAGIC   ) {
// MAGIC   /*
// MAGIC     Load data from object storage into a target delta table
// MAGIC     using Autoloader
// MAGIC   */
// MAGIC   
// MAGIC   var _df = genStream();
// MAGIC   var _query : StreamingQuery = _;
// MAGIC   
// MAGIC   def genStream () : DataFrame = {
// MAGIC     /* 
// MAGIC       Function to generate the write stream 
// MAGIC     */
// MAGIC 
// MAGIC     var df = spark.readStream
// MAGIC                   .format("cloudFiles")
// MAGIC                   .option("cloudFiles.format", fileType)
// MAGIC                   //.option("sep", ",") //seperator can be optional
// MAGIC                   .option("checkpointLocation", checkpoint_path)
// MAGIC                   .option("mergeSchema", "true")
// MAGIC                   .option("cloudFiles.schemaLocation", schema_location)
// MAGIC                   .option("cloudFiles.inferColumnTypes", "true")
// MAGIC                   .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
// MAGIC                   .option("header", "true")
// MAGIC                   .load(s"$landingzone_path/*.$fileType")
// MAGIC                   .withColumn("filePath",input_file_name())
// MAGIC                   .withColumn("fileread_ts",(unix_timestamp()*1000))
// MAGIC 
// MAGIC     _df = df.select(df.columns.map(x => col(x).as(x.toLowerCase)): _*)
// MAGIC     
// MAGIC     // return the dataframe
// MAGIC     _df
// MAGIC     }
// MAGIC   
// MAGIC   def startStreamSingleton() : StreamingQuery = {
// MAGIC     /*
// MAGIC       Function to start a write stream
// MAGIC       returns error on failure
// MAGIC     */
// MAGIC     try {
// MAGIC       _query = _df.writeStream
// MAGIC                   .format("delta")
// MAGIC                   .option("checkpointLocation", checkpoint_path)
// MAGIC                   .option("mergeSchema", "true")
// MAGIC                   .option("mode", "overwrite")
// MAGIC //                   .trigger(Trigger.Once)
// MAGIC                   .start(write_path)
// MAGIC     } catch {
// MAGIC       case _: Throwable => {
// MAGIC         println("Found Failure in writing stream")
// MAGIC       }
// MAGIC     }
// MAGIC       
// MAGIC     // Return the query
// MAGIC     _query
// MAGIC   }
// MAGIC   
// MAGIC   def startStream() : Unit = {
// MAGIC     /* 
// MAGIC       Function to start a write stream,
// MAGIC       will retry on failure
// MAGIC     */
// MAGIC     try {
// MAGIC       _query = startStreamSingleton()
// MAGIC       _query.awaitTermination()
// MAGIC     } catch {
// MAGIC       case _: Throwable => {
// MAGIC         case x : UnknownFieldException => {
// MAGIC           println("Found failure in writing stream, restarting")
// MAGIC           _df = genStream()
// MAGIC           startStream()
// MAGIC       }
// MAGIC     }
// MAGIC   }
// MAGIC   }
// MAGIC   
// MAGIC   def stopStream() : Unit = {
// MAGIC     /*
// MAGIC       Function to stop a write stream
// MAGIC     */
// MAGIC     
// MAGIC     try {
// MAGIC       // Stop the streaming query
// MAGIC       _query.stop()
// MAGIC     } catch {
// MAGIC       case _: Throwable => {
// MAGIC         println("Failed to stop stream")
// MAGIC       }
// MAGIC     }
// MAGIC     println("Stopped stream")
// MAGIC   }
// MAGIC 
// MAGIC }

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Setup

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC /*
// MAGIC   Setup some variables
// MAGIC */
// MAGIC 
// MAGIC // val rootBucket = "dbfs:/mnt/landing/testing"
// MAGIC val rootBucket = "dbfs:/tmp/autoloader_autorestart"
// MAGIC val dataBucket= s"$rootBucket/data"
// MAGIC val fileType = "json"
// MAGIC val checkpointPath = s"$rootBucket/checkpoints"
// MAGIC val schemaLocation = s"$rootBucket/schema_location"
// MAGIC val writePath = s"$rootBucket/delta_table"
// MAGIC val landingzonePath = dataBucket

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC /*
// MAGIC   Clean up directories
// MAGIC */
// MAGIC 
// MAGIC dbutils.fs.rm(checkpointPath, true)
// MAGIC dbutils.fs.rm(writePath, true)
// MAGIC dbutils.fs.rm(dataBucket, true)
// MAGIC dbutils.fs.rm(schemaLocation, true)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Data Generation

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC import org.apache.spark.sql.functions.col;
// MAGIC 
// MAGIC /*
// MAGIC   Create a dummy dataframe and write to parquet
// MAGIC */
// MAGIC 
// MAGIC val col_a = sc.range(1, 100000, 1).toDF("col_1")
// MAGIC val col_b = sc.range(1, 100000, 1).toDF("col_2")
// MAGIC val col_c = sc.range(1, 100000, 1).toDF("col_3")
// MAGIC 
// MAGIC val dummyDF1 = col_a.join(col_b, col_a("col_1") === col_b("col_2"))
// MAGIC val dummyDF2 = dummyDF1.join(col_c, dummyDF1("col_1") === col_c("col_3"))

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC /*
// MAGIC   Look at contents of dummyDF1
// MAGIC */
// MAGIC 
// MAGIC display(dummyDF1.orderBy("col_1"))

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC /*
// MAGIC   Write out data
// MAGIC */
// MAGIC 
// MAGIC dummyDF1.write.format(fileType).mode("append").save(dataBucket)

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC display(dbutils.fs.ls(landingzonePath))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Ingest Files

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC /*
// MAGIC   Start data ingestion
// MAGIC */
// MAGIC 
// MAGIC var delta_ingest = new DeltaLoader(fileType,
// MAGIC                                    checkpointPath,
// MAGIC                                    schemaLocation,
// MAGIC                                    landingzonePath,
// MAGIC                                    writePath)

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC /*
// MAGIC   Write to delta
// MAGIC */
// MAGIC 
// MAGIC delta_ingest.startStream()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Read files

// COMMAND ----------

// MAGIC %fs
// MAGIC 
// MAGIC ls dbfs:/mnt/landing/testing/delta_table

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC /*
// MAGIC   Get the files in the write path
// MAGIC */
// MAGIC 
// MAGIC display(spark.read.format("delta")
// MAGIC              .load(writePath))
