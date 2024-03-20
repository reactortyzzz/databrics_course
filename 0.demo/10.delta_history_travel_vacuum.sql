-- Databricks notebook source
DESC HISTORY f1_demo.drivers_merger

-- COMMAND ----------

select * from f1_demo.drivers_merger version as of 1

-- COMMAND ----------

select * from f1_demo.drivers_merger timestamp as of "2024-03-12T08:08:19.000+00:00" --insert timestamp from previous step

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format("delta").option("timestampAsOf", '2024-03-12T08:08:19.000+00:00').load("/mnt/databricsformula1dl/demo/drivers_merger")
-- MAGIC #the same can be done with versionAsOf

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

set spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM f1_demo.drivers_merger RETAIN 0 HOURS

-- COMMAND ----------

select * from f1_demo.drivers_merger timestamp as of "2024-03-12T08:08:19.000+00:00" --insert timestamp from previous step

-- COMMAND ----------

select * from f1_demo.drivers_merger

-- COMMAND ----------

desc history f1_demo.drivers_merger

-- COMMAND ----------

delete from  f1_demo.drivers_merger where driverId = 1

-- COMMAND ----------

desc history f1_demo.drivers_merger

-- COMMAND ----------

select * from f1_demo.drivers_merger version as of 5

-- COMMAND ----------

merge into f1_demo.drivers_merger tgt
using f1_demo.drivers_merger version as of 5 src
  on (tgt.driverId = src.driverId)
when not matched then 
  insert * 

-- COMMAND ----------

desc history f1_demo.drivers_merger

-- COMMAND ----------

select * from f1_demo.drivers_merger
