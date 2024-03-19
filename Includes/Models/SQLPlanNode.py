# Databricks notebook source
from pyspark.sql.functions import *
import json

# COMMAND ----------

class SQLPlanNode:

  def __init__(self, sql_node_obj):
    self.sql_node_obj = sql_node_obj

  def __str__(self):
    return json.dumps(self.sql_node_obj, indent=4)
  
  def get_name(self):
    return self.sql_node_obj['nodeName']
  
  def get_type(self):
    return self.get_name().split()[0]

  def is_photon(self):
    return self.get_type().lower().find('photon') != -1

  def is_scan_node(self):
    return self.get_type().lower().find('scan') != -1
  
  def get_scan_source(self):
    if not self.is_scan_node():
      return ""
    
    return self.get_name().split()[2]
  
  def get_scan_source_type(self):
    if not self.is_scan_node():
      return ""
    
    return self.get_name().split()[1]


# COMMAND ----------


