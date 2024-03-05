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
.json(f"{raw_folder_path}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed('resultId', 'result_id') \
.withColumnRenamed('raceId', 'race_id') \
.withColumnRenamed('driveId', 'driver_id') \
.withColumnRenamed('constructorId', 'constructor_id') \
.withColumnRenamed('positionText', 'position_text') \
.withColumnRenamed('positionOrder', 'position_order') \
.withColumnRenamed('fastestLap', 'fastest_lap') \
.withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
.withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed').withColumn("date_source", lit(v_data_source))


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

results_final_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

dbutils.notebook.exit("Success")
