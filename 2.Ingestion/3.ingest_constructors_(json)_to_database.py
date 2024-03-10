# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/karpeko1995@gmail.com/databrics_course/3.includes/1.configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/karpeko1995@gmail.com/databrics_course/3.includes/2.common_functions"

# COMMAND ----------

constructor_schema = 'constructorId INT, costructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

constructor_df = spark.read \
.schema(constructor_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### step2 - drop unwanted columns from the dataframe

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# other methods
#constructor_dropped_df = constructor_df.drop(constructor_df['url'])

#from pyspark.sql.funsctions import col
#constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### step3 - rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constuctors_final_df = constructor_dropped_df.withColumnRenamed('constructorId', 'constructor_id') \
.withColumnRenamed('constructorRef', 'constructor_ref').withColumn("date_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

timestamp_df = add_ingestion_date(constuctors_final_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### step4 - write output to parquet file

# COMMAND ----------

timestamp_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
