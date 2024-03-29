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
# MAGIC ####Incremental load - Method 1

# COMMAND ----------

#method 1 of incremental load
#solution made in a way so that when we re-run a notebook we won't have duplicates, that's why we check if there are existing partitions and if there are - we drop them
#the other one check here is checking the existing of the table and only on existing table we make the changes
#collect converts data to a list - be careful with large datasets because it put data in worker memory
for race_id_list in results_final_df.select("race_id").distinct().collect():
    if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
        spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

#here we make append, not overwrite
results_final_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id , count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc

# COMMAND ----------

dbutils.notebook.exit("Success")
