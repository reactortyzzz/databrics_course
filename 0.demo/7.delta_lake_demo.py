# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/databricsformula1dl/demo'

# COMMAND ----------

# MAGIC %sql 
# MAGIC --drop table f1_demo.results_managed

# COMMAND ----------

results_df = spark.read \
    .option("inferSchema", True) \
    .json("/mnt/databricsformula1dl/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/databricsformula1dl/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table f1_demo.results_external
# MAGIC using delta
# MAGIC location "/mnt/databricsformula1dl/demo/results_external"

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/databricsformula1dl/demo/results_external")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo.results_partitioned
