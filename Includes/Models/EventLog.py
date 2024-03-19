# Databricks notebook source
import json
from pyspark.sql.functions import *
from pathlib import Path

class EventLog:

  # def __init__(self, path, use_schema=True, schema_path = Path(__file__).parent.resolve() / + './eventlog_schema.json'):
  def __init__(self, path, use_schema=True, schema_path = './eventlog_schema.json'):
    self.path = path

    if use_schema:
      with open(schema_path, 'r') as f:
        eventlog_schema_json = f.read()
        eventlog_schema = StructType.fromJson(json.loads(eventlog_schema_json))

      self.df = spark.read.json(self.path, schema=eventlog_schema)
    else:
      self.df = spark.read.json(self.path)

    self.df_cached = self.df.cache()

    self.df_cached.createOrReplaceTempView('EventLog')

  def display(self):
    self.df_cached.display()

  def filter_events_df(self, event_types):
    condition = (col('event') == event_types.pop())
    for event_type in event_types:
      condition |= (col('event') == event_type)
    
    return self.df_cached.filter(condition)


# COMMAND ----------


