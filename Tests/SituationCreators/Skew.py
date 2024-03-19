# Databricks notebook source
from pyspark.sql.functions import *

spark.table('hive_metastore.sternp.flights').filter(col('year') < 1990).repartition(64, col('TailNum')).write.mode('overwrite').saveAsTable('sternp.tmp.skew_write')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table sternp.tmp.skew_write
