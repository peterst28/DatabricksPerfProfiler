# Databricks notebook source
for i in range(0,100):
  sql("select * from hive_metastore.sternp.flights limit 1000").collect()

# COMMAND ----------


