# Databricks notebook source
class Executors:

  def __init__(self, event_log):
    self.event_log = event_log
    self.df = self.event_log.filter_events_df(['SparkListenerExecutorAdded'])
    self.df_cached = self.df.cache()
    self.df_cached.createOrReplaceTempView('Executors')

  def display(self):
    self.df_cached.display()

  def get_num_cores_per_executor(self):
    return self.df_cached.select('`Executor Info`.`Total Cores`').collect()[0]['Total Cores']
  
  
