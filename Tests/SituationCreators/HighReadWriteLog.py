# Databricks notebook source
spark.table('hive_metastore.sternp.flights').write.mode('overwrite').saveAsTable('sternp.tmp.readwrite')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table sternp.tmp.readwrite

# COMMAND ----------


