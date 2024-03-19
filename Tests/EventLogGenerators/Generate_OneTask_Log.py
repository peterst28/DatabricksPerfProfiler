# Databricks notebook source
dbutils.notebook.run('../SituationCreators/OneTask', 300)

# COMMAND ----------

dbutils.notebook.run('./SaveLog', 300, {"destination": "../EventLogs/OneTask"})
