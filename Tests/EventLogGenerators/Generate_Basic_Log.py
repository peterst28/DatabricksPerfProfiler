# Databricks notebook source
dbutils.notebook.run('../SituationCreators/BasicLog', 300)

# COMMAND ----------

dbutils.notebook.run('./SaveLog', 300, {"destination": "../EventLogs/Basic"})
