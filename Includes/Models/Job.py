# Databricks notebook source
class Job:

  def __init__(self, job_obj):
    self.job_obj = job_obj

  def get_description(self):
    return self.job_obj['Description']
  
  def get_notebook_path(self):
    return self.job_obj['Notebook_Path']

# COMMAND ----------


