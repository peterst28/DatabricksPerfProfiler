# Databricks notebook source
try:
  dbutils.notebook.run('../SituationCreators/ExecutorFail', 300)
except:
  pass

# COMMAND ----------

dbutils.notebook.run('./SaveLog', 300, {"destination": "../EventLogs/ExecutorFail"})
