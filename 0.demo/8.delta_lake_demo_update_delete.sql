-- Databricks notebook source
select * from f1_demo.results_managed

-- COMMAND ----------

UPDATE f1_demo.results_managed
  SET points = 11 - position
where position <= 10

-- COMMAND ----------

select * from f1_demo.results_managed

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import DeltaTable
-- MAGIC
-- MAGIC deltaTable = DeltaTable.forPath(spark, "/mnt/databricsformula1dl/demo/results_managed")
-- MAGIC deltaTable.update("position <= 10" , {"points": "'21 - position'"})
-- MAGIC
-- MAGIC

-- COMMAND ----------

select * from f1_demo.results_managed

-- COMMAND ----------

DELETE FROM f1_demo.results_managed 
WHERE position > 10

-- COMMAND ----------

select * from f1_demo.results_managed

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import DeltaTable
-- MAGIC
-- MAGIC deltaTable = DeltaTable.forPath(spark, "/mnt/databricsformula1dl/demo/results_managed")
-- MAGIC
-- MAGIC # Declare the predicate by using a SQL-formatted string.
-- MAGIC deltaTable.delete("points = 0")

-- COMMAND ----------

select * from f1_demo.results_managed
