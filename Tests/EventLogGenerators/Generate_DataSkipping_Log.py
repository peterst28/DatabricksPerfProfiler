# Databricks notebook source
dbutils.notebook.run('../SituationCreators/DataSkipping', 300)

# COMMAND ----------

dbutils.notebook.run('./SaveLog', 300, {"destination": "../EventLogs/DataSkipping"})
