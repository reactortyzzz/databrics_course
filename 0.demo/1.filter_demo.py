# Databricks notebook source
# MAGIC %run "/Workspace/Repos/karpeko1995@gmail.com/databrics_course/3.includes/1.configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

races_filtered_df = races_df.filter("race_year = 2019 and round <= 5")

# COMMAND ----------

races_filtered_df = races_df.filter( (races_df["race_year"] == 2019) & (races_df["round"] <=5) )

#where is the same as filter
#races_filtered_df = races_df.where( (races_df["race_year"] == 2019) & (races_df["round"] <=5) )

# COMMAND ----------

display(races_df)
