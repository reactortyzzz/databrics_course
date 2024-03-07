# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/karpeko1995@gmail.com/databrics_course/3.includes/1.configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/karpeko1995@gmail.com/databrics_course/3.includes/2.common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

races_schema = StructType(
    fields = [
               StructField("raceId", IntegerType(), False), #last parameter False is nullable or not
               StructField("year", IntegerType(), True),
               StructField("round", IntegerType(), True),
               StructField("circuitId", IntegerType(), True),
               StructField("name", StringType(), True),
               StructField("date",DateType(), True),
               StructField("time", StringType(), True),
               StructField("url", StringType(), True)
        ] 
)



# COMMAND ----------

# main calling method
races_df = spark.read \
.option("header", True) \
.option("schema",races_schema ) \
.csv(f"{raw_folder_path}/races.csv")  

# COMMAND ----------

timestamp_df = add_ingestion_date(races_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### select only specific columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat, col

# COMMAND ----------

races_timestamp = timestamp_df.withColumn("ingestion_date",current_timestamp()) \
.withColumn( \
    'race_timestamp', 
    to_timestamp(
        concat( col('date'), lit(' '), col('time')),
        'yyyy-MM-dd HH:mm:ss')
).withColumn("date_source", lit(v_data_source))

# COMMAND ----------

from pyspark.sql.functions import col   

# COMMAND ----------

#this method allows not only select but do futher trans on columns like renaming
races_final_df = races_timestamp.select ( 
    col("raceId").alias("race_id"), 
    col("year").alias("race_year"),
    col("round"),
    col("circuitId").alias("circuit_id"),
    col("name"),
    col("ingestion_date"),
    col("race_timestamp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step4 - add ingestion date to the dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC #### write data to datalake as parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")
