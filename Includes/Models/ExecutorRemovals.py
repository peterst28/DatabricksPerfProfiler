# Databricks notebook source
class ExecutorRemovals:

  def __init__(self, event_log):
    self.event_log = event_log
    self.raw_df = self.event_log.filter_events_df(['SparkListenerExecutorRemoved'])
    self.raw_df.createOrReplaceTempView('executor_removals_raw')

    sql("""CREATE OR REPLACE TEMP VIEW ExecutorRemoved AS
          select
            `Removed Reason` as Reason,
            Timestamp
          from executor_removals_raw"""
    )

    self.df = spark.table('ExecutorRemoved')
    self.df_cached = self.df.cache()

  def display(self):
    self.df_cached.display()

  def get_executor_fail_df(self):
    return sql('select * from ExecutorRemoved where Reason NOT LIKE "Executor decommission: worker decommissioned because of kill request from HTTP endpoint%"')

  def count_executor_fails(self):
    return self.get_executor_fail_df().count()
    
  def get_executor_fail_error(self):
    return self.get_executor_fail_df().select('Reason').first()['Reason']

# COMMAND ----------


