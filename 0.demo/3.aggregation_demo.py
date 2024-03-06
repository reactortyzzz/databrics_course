# Databricks notebook source
# MAGIC %run "/Workspace/Repos/karpeko1995@gmail.com/databrics_course/3.includes/1.configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ### agg demo

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results.printSchema()

# COMMAND ----------

demo_df = race_results.filter("race_year = 2020")

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'") \
    .select(sum("points"), countDistinct("race_name")) \
    .withColumnRenamed("sum(points)", "total_points") \
    .withColumnRenamed("count(DISTINCT race_name)", "number_of_races") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group by method

# COMMAND ----------

#you cannot apply several aggregation methods using regular group by
#we need to use method pyspark.sql.GroupedData.agg
demo_df.groupBy("driver_name") \
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window Functions 

# COMMAND ----------

demo_df = race_results.filter("race_year in (2019,2020)")

# COMMAND ----------

from pyspark.sql.functions import col, rank, desc
from pyspark.sql.window import Window

# COMMAND ----------

demo_grouped_df = demo_df.groupBy("race_year", "driver_name") \
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

type(demo_grouped_df)

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"))


# COMMAND ----------

demo_grouped_df.withColumn("rank", rank().over(driver_rank_spec)).show(100)
