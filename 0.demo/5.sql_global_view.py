# Databricks notebook source
# MAGIC %run "/Workspace/Repos/karpeko1995@gmail.com/databrics_course/3.includes/1.configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Global sql view
# MAGIC we can access view using usual SQL or python commands. The difference and use cases for it is that using python we can create dataframe and use some dynamic variables. 
# MAGIC Global views can be referenced via all notebooks which are attached to a cluster where this global view is attached

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;
# MAGIC --important note here is that we need to specify kind of database name in order to reach table in global temp views

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1)
# MAGIC FROM global_temp.gv_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

#we can use variable/parameters in python SQL
p_race_year = 2020

# COMMAND ----------

race_results_2019_df = spark.sql(f"SELECT * FROM global_temp.gv_race_results WHERE race_year = {p_race_year}")

# COMMAND ----------

display(race_results_2019_df)
