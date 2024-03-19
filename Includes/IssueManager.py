# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

class IssueManager:

  def __init__(self):
    self.issues = []

  def add_issue(self, name, description, severity, time_lost_secs, recommended_resolution, notebook_path = None, snippet = None, execution_plan = None):
    self.issues.append((name, description, severity, time_lost_secs, recommended_resolution, notebook_path, snippet, execution_plan))

  def get_df(self):
    schema = (
      StructType(
        [
          StructField('Name', StringType(), True), 
          StructField('Description', StringType(), True), 
          StructField('Severity', StringType(), True), 
          StructField('Time_Lost_Secs', DoubleType(), True), 
          StructField('Recommended_Resolution', StringType(), True), 
          StructField('Notebook_Path', StringType(), True), 
          StructField('Snippet', StringType(), True),
          StructField('Execution_Plan', StringType(), True)
        ]
      ))

    return spark.createDataFrame(self.issues, schema=schema)
  
  def clear(self):
    self.issues = []

# COMMAND ----------


