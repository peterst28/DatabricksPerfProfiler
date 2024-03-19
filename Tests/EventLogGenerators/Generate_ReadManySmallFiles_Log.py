# Databricks notebook source
dbutils.notebook.run('../SituationCreators/ReadManySmallFiles', 1000)

# COMMAND ----------

dbutils.notebook.run('./SaveLog', 300, {"destination": "../EventLogs/ReadManySmallFiles"})
