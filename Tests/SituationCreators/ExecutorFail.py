# Databricks notebook source
from pyspark.sql.functions import *

spark.range(0,10000000000).select(median('id')).display()

# COMMAND ----------


