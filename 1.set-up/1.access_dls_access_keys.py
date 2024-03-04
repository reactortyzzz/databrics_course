# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure data lake using access keys
# MAGIC 1. set the spark config fs.azure.account.key
# MAGIC 1. list files from demo container
# MAGIC 1. read data from circuits.csv file
# MAGIC   

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.databricsformula1dl.dfs.core.windows.net",
    "HrECrCptWezHQFsVHJMcADS/WaxV5Nv+PpBx7bepg1wT8Kx+/UBOXcCsZ1ZpkQcn5V4h2YDox6Qj+AStfOuaXA=="
)

# COMMAND ----------

display(
    dbutils.fs.ls("abfss://demo@databricsformula1dl.dfs.core.windows.net")
)

# COMMAND ----------

display(
    spark.read.csv("abfss://demo@databricsformula1dl.dfs.core.windows.net/circuits.csv")
)
