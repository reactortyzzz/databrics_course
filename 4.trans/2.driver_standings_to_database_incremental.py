# Databricks notebook source
# MAGIC %run "/Workspace/Repos/karpeko1995@gmail.com/databrics_course/3.includes/1.configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/karpeko1995@gmail.com/databrics_course/3.includes/2.common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_list = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'") \
.select("race_year") \
.distinct().collect()

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

# COMMAND ----------

print(race_year_list)

# COMMAND ----------

from pyspark.sql.functions import col

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

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

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

overwrite_partition(final_df, "f1_presentation", "driver_standings", "race_year")
