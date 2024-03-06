# Databricks notebook source
# MAGIC %run "/Workspace/Repos/karpeko1995@gmail.com/databrics_course/3.includes/1.configuration"

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import count, sum, col, rank, desc, when
from pyspark.sql.window import Window

# COMMAND ----------

grouped_df = race_results.groupBy("race_year", "driver_name", "driver_nationality", "team") \
    .agg(sum("points").alias("total_points"), 
         count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))

# COMMAND ----------

final_df = grouped_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

race_results.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")
