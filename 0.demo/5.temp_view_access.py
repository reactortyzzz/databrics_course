# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * from v_race_results
# MAGIC --local temp view - we cannot reach it from different notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from global_temp.gv_race_results
# MAGIC --global temp view - we reach it from different notebook in the same cluster
