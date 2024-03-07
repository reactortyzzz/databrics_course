-- Databricks notebook source
create database if not exists f1_processed
location "/mnt/databricsformula1dl/processed"
--we specify location so that it would be created under processed folder, not default location

-- COMMAND ----------

--to test test_brach
describe database  f1_processed
