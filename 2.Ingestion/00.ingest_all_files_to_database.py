# Databricks notebook source
v_result = dbutils.notebook.run("/Workspace/Repos/karpeko1995@gmail.com/databrics_course/2.Ingestion/1.ingest_circuits_file_to_database", 0,{"p_data_source":"Ergast API", "p_file_date":"2021-04-18"}) 


# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("/Workspace/Repos/karpeko1995@gmail.com/databrics_course/2.Ingestion/2.ingest_races_file_to_database", 0,{"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("/Workspace/Repos/karpeko1995@gmail.com/databrics_course/2.Ingestion/3.ingest_constructors_(json)_to_database", 0,{"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("/Workspace/Repos/karpeko1995@gmail.com/databrics_course/2.Ingestion/4.ingest_drivers_json_multiline_to_database", 0,{"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("/Workspace/Repos/karpeko1995@gmail.com/databrics_course/2.Ingestion/5.ingest_results_json_to_database_incremental2_dynamic", 0,{"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("/Workspace/Repos/karpeko1995@gmail.com/databrics_course/2.Ingestion/6.ingest_pitstops_json_multiline_to_database", 0,{"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("/Workspace/Repos/karpeko1995@gmail.com/databrics_course/2.Ingestion/7.ingest_laptime_csv_folder_to_database", 0,{"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("/Workspace/Repos/karpeko1995@gmail.com/databrics_course/2.Ingestion/8.ingest_qualifying_json_folder_to_database", 0,{"p_data_source":"Ergast API", "p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.qualifying
# MAGIC group by race_id
# MAGIC order by race_id desc
