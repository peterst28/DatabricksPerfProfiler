# Databricks notebook source
from pyspark.sql.types import LongType

def throwError():
  10 / 0

throw_error_udf = udf(throwError, LongType())

# COMMAND ----------

try:
  spark.range(0,10).withColumn('test', throw_error_udf()).collect()
except Exception as e:
  print(e)

# COMMAND ----------

print('foo')
