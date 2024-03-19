# Databricks notebook source
spark.table('hive_metastore.sternp.flights').limit(2000).write.mode('overwrite').partitionBy('id').saveAsTable('sternp.tmp.smallfiles')

# COMMAND ----------


