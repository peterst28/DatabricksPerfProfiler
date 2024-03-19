# Databricks notebook source
# MAGIC %md
# MAGIC This script is intended to be used with cluster log delivery

# COMMAND ----------

dbutils.notebook.run('../SituationCreators/HighReadWriteLog', 1000)
