# Databricks notebook source
dbutils.notebook.run('../SituationCreators/JobGaps', 300)

# COMMAND ----------

dbutils.notebook.run('./SaveLog', 300, {"destination": "../EventLogs/JobGaps"})
