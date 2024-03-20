# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest results json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 - read json using spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/karpeko1995@gmail.com/databrics_course/3.includes/1.configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/karpeko1995@gmail.com/databrics_course/3.includes/2.common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType   

# COMMAND ----------

results_schema = StructType(
    fields = [
               StructField("resultId", IntegerType(), False), #last parameter False is nullable or not
               StructField("raceId", IntegerType(), True),
               StructField("driverId", IntegerType(), True),
               StructField("constructorId", IntegerType(), True),
               StructField("number", IntegerType(), True),
               StructField("grid",IntegerType(), True),
               StructField("position", IntegerType(), True),
               StructField("positionText",StringType(), True),
               StructField("positionOrder",IntegerType(), True),
               StructField("points",FloatType(), True),
               StructField("laps",IntegerType(), True),
               StructField("time",StringType(), True),
               StructField("milliseconds",IntegerType(), True),
               StructField("fastestLap",IntegerType(), True),
               StructField("rank",IntegerType(), True),
               StructField("fastestLapTime",StringType(), True),
               StructField("fastestLapSpeed",FloatType(), True),
               StructField("statusId",StringType(), True)
        ] 
)



# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed('resultId', 'result_id') \
.withColumnRenamed('raceId', 'race_id') \
.withColumnRenamed('driverId', 'driver_id') \
.withColumnRenamed('constructorId', 'constructor_id') \
.withColumnRenamed('positionText', 'position_text') \
.withColumnRenamed('positionOrder', 'position_order') \
.withColumnRenamed('fastestLap', 'fastest_lap') \
.withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
.withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed') \
.withColumn("date_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

timestamp_df = add_ingestion_date(results_with_columns_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Drop unwanted columns

# COMMAND ----------

results_final_df = timestamp_df.drop('statusId')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC #second method
# MAGIC spark expects partition column to be the last one
# MAGIC also this method should have better perfomance as we do not manualy check for partition to drop and replace but letting spark to do so

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table f1_processed.results

# COMMAND ----------

#   overwrite_partition(results_final_df, "f1_processed", "results", "race_id")

# COMMAND ----------

# this is a initial code before we made a function out of it

# spark.conf.set("spark.databrics.optimizer.dynamicPartitionPruning", "true")

# from delta.tables import DeltaTable

# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     deltaTable = DeltaTable.forPath(spark, "/mnt/databricsformula1dl/processed/results")
#     deltaTable.alias("tgt").merge(
#         results_final_df.alias("src"),
#         "tgt.result_id = src.result_id AND tgt.race_id = src.race_id") \
#         .whenMatchedUpdateAll() \
#         .whenNotMatchedInsertAll() \
#         .execute()
# else:
#     results_final_df.write.mode("overwrite").partitionBy("race_id").format("delta").saveAsTable("f1_processed.results")

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_final_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id , count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

dbutils.notebook.exit("Success")
