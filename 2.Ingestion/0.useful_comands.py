# Databricks notebook source
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
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv('dbfs:/mnt/databricsformula1dl/raw/circuits.csv')  

# COMMAND ----------

# brics can identify schema itself, but this should be used only in testing purposes and on small amount of data, as it increases # of jobes and can fail
circuits_df = spark.read \
.option("header", True) \
#this line does it
.option("inferSchema", True) \ 
.csv('dbfs:/mnt/databricsformula1dl/raw/circuits.csv')  

# COMMAND ----------

#to check that the type of variable is dataframe
type(circuits_df)

# COMMAND ----------

circuits_df.show(20)

# COMMAND ----------

# another way to show data in more nicer way
display(circuits_df)

# COMMAND ----------

# to see the schema of dataframe - as we can see - everything should be strings and that is not right
circuits_df.printSchema()

# COMMAND ----------

# databrics can analyze data types
circuits_df.describe().show()

# COMMAND ----------

display(dbutils.fs.mounts())
# /mnt/databricsformula1dl/raw

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/databricsformula1dl/raw
# MAGIC
