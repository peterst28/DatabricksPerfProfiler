# Databricks notebook source
spark.table('sternp.tmp.smallfiles').write.mode('overwrite').format('noop').save()

# COMMAND ----------


