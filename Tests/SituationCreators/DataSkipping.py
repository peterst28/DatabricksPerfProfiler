# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

spark.table('hive_metastore.sternp.flights').filter(col('id') == 1234).write.mode('overwrite').format('noop').save()

# COMMAND ----------


