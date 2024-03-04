# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 - read json using spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../3.includes/1.configuration"

# COMMAND ----------

# MAGIC %run "../3.includes/2.common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType   

# COMMAND ----------

name_schema = StructType(
    fields = [
               StructField("forename", StringType(), True), #last parameter False is nullable or not
               StructField("surname", StringType(), True)
            ] 
)


# COMMAND ----------

driver_schema = StructType(
    fields = [
               StructField("driverId", IntegerType(), False), #last parameter False is nullable or not
               StructField("driverRef", StringType(), True),
               StructField("code", IntegerType(), True),
               StructField("name", name_schema, True),
               StructField("dob", StringType(), True),
               StructField("nationality",StringType(), True),
               StructField("url", StringType(), True)
        ] 
)



# COMMAND ----------

driver_df = spark.read \
.schema(driver_schema) \
.json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

driver_with_columns_df = driver_df.withColumnRenamed('driverId', 'driver_id') \
.withColumnRenamed('driverRef', 'driver_ref') \
.withColumn('name', concat( col( 'name.forename'), lit(' '), col('name.surname'))).withColumn("date_source", lit(v_data_source))

# COMMAND ----------

timestamp_df = add_ingestion_date(driver_with_columns_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Drop unwanted columns

# COMMAND ----------

driver_final_df = timestamp_df.drop('url')


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

driver_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

dbutils.notebook.exit("Success")
