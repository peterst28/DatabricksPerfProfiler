# Databricks notebook source
from pyspark.sql.functions import *
import json

# COMMAND ----------

class SQLPlan:

  def __init__(self, sql_plan_json):

    self.sql_plan = json.loads(sql_plan_json)

  def __str__(self):
    return json.dumps(self.sql_plan, indent=4)
  
  def get_nodes_with_accumulators(self, accumulator_ids):
    matching_nodes = []
    self._get_nodes_with_accumulators_recurse(self.sql_plan, accumulator_ids, matching_nodes)
    return matching_nodes

  def _get_nodes_with_accumulators_recurse(self, sql_plan, accumulator_ids, matching_nodes):
    
    for child in sql_plan['children']:
      self._get_nodes_with_accumulators_recurse(child, accumulator_ids, matching_nodes)

    for metric in sql_plan['metrics']:
      if metric['accumulatorId'] in accumulator_ids:
        matching_nodes.append(SQLPlanNode(sql_plan))
        break

# COMMAND ----------


