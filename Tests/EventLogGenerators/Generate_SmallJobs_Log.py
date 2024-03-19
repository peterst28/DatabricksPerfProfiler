# Databricks notebook source
dbutils.notebook.run('../SituationCreators/ManySmallJobs', 3000)

# COMMAND ----------

dbutils.notebook.run('./SaveLog', 300, {"destination": "../EventLogs/ManySmallJobs"})
