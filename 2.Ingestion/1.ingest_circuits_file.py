# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/karpeko1995@gmail.com/databrics_course/3.includes/1.configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/karpeko1995@gmail.com/databrics_course/3.includes/2.common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields = [
               StructField("circuitId", IntegerType(), False), #last parameter False is nullable or not
               StructField("circuitRef", StringType(), True),
               StructField("name", StringType(), True),
               StructField("location", StringType(), True),
               StructField("country", StringType(), True),
               StructField("lat", DoubleType(), True),
               StructField("lng", DoubleType(), True),
               StructField("alt", IntegerType(), True),
               StructField("url", StringType(), True)
    ] 
)



# COMMAND ----------

# main calling method
circuits_df = spark.read.option("header", True).schema(circuits_schema).csv(f'{raw_folder_path}/circuits.csv')  

# COMMAND ----------

# MAGIC %md
# MAGIC #### select only specific columns
# MAGIC

# COMMAND ----------

circuits_selected_df = circuits_df.select ( "circuitId", "circuitRef",
    "name",
     "location",
    "country",
    "lat",
    "lng",
    "alt",
)
 

# COMMAND ----------

circuits_selected_df2 = circuits_df.select ( 
    circuits_df.circuitId, 
    circuits_df.circuitRef,
    circuits_df.name,
    circuits_df.location,
    circuits_df.country,
    circuits_df.lat,
    circuits_df.lng,
    circuits_df.alt,
)

# COMMAND ----------

circuits_selected_df3 = circuits_df.select ( 
    circuits_df["circuitId"], 
    circuits_df["circuitRef"],
    circuits_df["name"],
    circuits_df["location"],
    circuits_df["country"],
    circuits_df["lat"],
    circuits_df["lng"],
    circuits_df["alt"]
)

# COMMAND ----------

from pyspark.sql.functions import col   

# COMMAND ----------

#this method allows not only select but do futher trans on columns like renaming
circuits_selected_df4 = circuits_df.select ( 
    col("circuitId"), 
    col("circuitRef"),
    col("name"),
    col("location"),
    ##col("country").alias("race_country"),
    col("country"),
    col("lat"),
    col("lng"),
    col("alt")
)

# COMMAND ----------

# MAGIC %md
# MAGIC step3 rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit


# COMMAND ----------

circuits_renamed_df = circuits_selected_df4.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude").withColumn("date_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step4 - add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp##, lit

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df) 
##\
##.withColumn ("env", lit("Production")) #this is how we can add additionla column with string

# COMMAND ----------

# MAGIC %md
# MAGIC #### write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")
