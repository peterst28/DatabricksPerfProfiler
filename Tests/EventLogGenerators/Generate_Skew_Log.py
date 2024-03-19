# Databricks notebook source
dbutils.notebook.run('../SituationCreators/Skew', 900)

# COMMAND ----------

dbutils.notebook.run('./SaveLog', 300, {"destination": "../EventLogs/Skew"})
