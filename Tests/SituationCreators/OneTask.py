# Databricks notebook source
# MAGIC %sql
# MAGIC select distinct(year) from hive_metastore.sternp.flights

# COMMAND ----------

sql("select row_number() OVER (ORDER BY DepTime) from hive_metastore.sternp.flights where year < 1991").write.format('noop').mode('overwrite').save()

# COMMAND ----------


