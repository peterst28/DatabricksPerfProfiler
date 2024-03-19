# Databricks notebook source
dbutils.notebook.run('../SituationCreators/WriteManySmallFiles', 1000)

# COMMAND ----------

dbutils.notebook.run('./SaveLog', 300, {"destination": "../EventLogs/WriteManySmallFiles"})
