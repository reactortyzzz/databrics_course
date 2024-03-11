-- Databricks notebook source
--drop table f1_presentation.race_results;
--drop database f1_presentation;
create database if not exists f1_presentation
location "/mnt/databricsformula1dl/presentation"
--we specify location so that it would be created under processed folder, not default location

-- COMMAND ----------

describe database  f1_presentation
