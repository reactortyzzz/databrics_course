-- Databricks notebook source
use f1_processed;

-- COMMAND ----------

create table f1_presentation.calculated_race_results
using parquet
as 
select 
  b.race_year
  ,d.name as driver_name 
  ,c.name as team_name
  ,a.position
  ,a.points
  , (11 - a.position) as calculated_points
from f1_processed.results a
  join f1_processed.races b on (a.race_id = b.race_id)
  join f1_processed.constructors c on (a.constructor_id = c.constructor_id)
  join f1_processed.drivers d on (a.driver_id = d.driver_id)
--  join f1_processed.circuits e on (b.circuit_id = e.circuit_id)
where a.position < 11

