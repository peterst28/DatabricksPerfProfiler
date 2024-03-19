# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

class Tasks:

  def __init__(self, event_log):
    self.event_log = event_log
    self.raw_df = self.event_log.filter_events_df(['SparkListenerTaskStart', 'SparkListenerTaskEnd'])
    self.raw_df.createOrReplaceTempView('tasks_raw')

    sql("""CREATE OR REPLACE TEMP VIEW Tasks AS
      SELECT
        `Task Info`.`Task ID` as ID,
        `Stage ID` as StageID, 
        `Stage Attempt ID` as StageAttemptID, 
        `Task Metrics`.`Executor Run Time` as Run_Time
        
        FROM tasks_raw
        WHERE Event = 'SparkListenerTaskEnd'
        """
    )

    self.df = spark.table('Tasks')
    self.df_cached = self.df.cache()

  def display(self):
    self.df_cached.display()

  def get_task_runtimes(self, stage_id):
    tasks = self.df_cached.filter(col('StageID') == stage_id).select('Run_time').collect()
    run_times = [task['Run_time'] / 1000 for task in tasks]

    return run_times


 

# COMMAND ----------


