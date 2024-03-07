# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest results csv folder
# MAGIC

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f"{raw_folder_path}/lap_times/*.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id").withColumn("date_source", lit(v_data_source))

# COMMAND ----------

timestamp_df = add_ingestion_date(final_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

timestamp_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

dbutils.notebook.exit("Success")
