# Databricks notebook source
dbutils.notebook.run('../SituationCreators/ShuffleWrite', 1000)

# COMMAND ----------

dbutils.notebook.run('./SaveLog', 300, {"destination": "../EventLogs/ShuffleWrite"})
