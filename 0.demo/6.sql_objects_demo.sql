-- Databricks notebook source
create database demo;

-- COMMAND ----------

create database if not exists demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database demo;

-- COMMAND ----------

describe database extended demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

use demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

-- MAGIC %run "/Workspace/Repos/karpeko1995@gmail.com/databrics_course/3.includes/1.configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

use demo;
create or replace table demo.race_results_sql
as
select * from demo.race_results_python 

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

describe extended demo.race_results_python

-- COMMAND ----------

select * from demo.race_results_python
where race_year = 2020

-- COMMAND ----------

drop table if exists demo.race_results_sql
--dropping managed table also dropp meta data in metastore

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## External tables
-- MAGIC ####Create external tables using SQL
-- MAGIC ####Create external tables using Python
-- MAGIC ####Effect of dropping external table 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ##slightly different syntax here
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_python").saveAsTable("demo.race_results_ext_python")

-- COMMAND ----------

desc extended demo.race_results_ext_python;

-- COMMAND ----------

drop table if exists demo.race_results_ext_sql;

-- COMMAND ----------

--we created managed table using CETAS here is another way
create table demo.race_results_ext_sql
(
  race_year int
  ,race_name string
  ,race_date timestamp
  ,circuit_location string
  ,driver_name string
  ,driver_number int
  ,driver_nationality string
  ,team string
  ,grid int
  ,faster_lap int
  ,race_time string
  ,point float
  ,position int
  ,created_date timestamp
)
using parquet
--if it was managed table we could stop here, but for external we need to specify the location
location "dbfs:/mnt/databricsformula1dl/presentation/race_results_ext_sql"

-- COMMAND ----------

--we can drop a table, but just executing statement above we would still have the data even though we didn't insert anything - as data already placed in datalake

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

select count(1) from demo.race_results_ext_sql

-- COMMAND ----------

insert into demo.race_results_ext_sql
select * from demo.race_results_ext_python where race_year = 2020

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

drop table demo.race_results_ext_sql;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

--even we dropped table but it stil remains in datalake, we can drop data from datalake as well, but it's just to understand the diff between managed and external tables 
-- so for managed table spark take care of data and metadata
-- in case of external data spark manages only metadata, and data layer is on our side
