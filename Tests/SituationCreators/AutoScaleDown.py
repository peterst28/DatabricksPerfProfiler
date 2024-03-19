# Databricks notebook source
spark.table('hive_metastore.sternp.flights').write.format('noop').mode('overwrite').save()

# COMMAND ----------


