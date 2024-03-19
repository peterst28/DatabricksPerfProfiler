# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

class Stages:

  def __init__(self, event_log):
    self.event_log = event_log
    self.raw_df = self.event_log.filter_events_df(['SparkListenerStageSubmitted', 'SparkListenerStageCompleted'])
    self.raw_df.createOrReplaceTempView('stages_raw')

    sql("""CREATE OR REPLACE TEMP VIEW Stages AS
      SELECT
        ID,
        StageInfo_Submitted.`Submission Time` as Submission_Time,
        StageInfo_Completed.`Completion Time` as Completion_Time,
        StageInfo_Completed.`Completion Time` - StageInfo_Submitted.`Submission Time` as Run_Time,
        StageInfo_Completed.`Number of Tasks` as Num_Tasks,
        Properties.`spark.sql.execution.id` as SQL_ID,
        filter(StageInfo_Completed.Accumulables, 
          accumulable -> accumulable.Name == "internal.metrics.executorRunTime")[0].Value as Executor_Time,
        filter(StageInfo_Completed.Accumulables, 
          accumulable -> accumulable.Name == "internal.metrics.diskBytesSpilled")[0].Value as Disk_Bytes_Spill,
        filter(StageInfo_Completed.Accumulables, 
          accumulable -> accumulable.Name == "internal.metrics.input.bytesRead")[0].Value as BytesRead,
        filter(StageInfo_Completed.Accumulables, 
          accumulable -> accumulable.Name == "internal.metrics.output.bytesWritten")[0].Value as BytesWritten,
        filter(StageInfo_Completed.Accumulables, 
          accumulable -> accumulable.Name == "internal.metrics.shuffle.write.bytesWritten")[0].Value as ShuffleBytesWritten,
        (
          filter(StageInfo_Completed.Accumulables, 
            accumulable -> accumulable.Name == "internal.metrics.shuffle.read.localBytesRead")[0].Value
          + 
          filter(StageInfo_Completed.Accumulables, 
            accumulable -> accumulable.Name == "internal.metrics.shuffle.read.remoteBytesRead")[0].Value
        ) as ShuffeBytesRead,
        filter(StageInfo_Completed.Accumulables, 
          accumulable -> accumulable.Name == "files written")[0].Value as NumFilesWritten,
        filter(StageInfo_Completed.Accumulables, 
          accumulable -> accumulable.Name == "size of row groups before filtering")[0].Value as TotalRowGroupsSize,
        filter(StageInfo_Completed.Accumulables, 
          accumulable -> accumulable.Name == "row groups in files to read total")[0].Value as RowGroupsToRead,
        StageInfo_Completed.Accumulables,
        StageInfo_Completed.`Failure Reason` as Failure_Reason,
        Properties,
        StageInfo_Submitted,
        StageInfo_Completed
        FROM (
          select
            ID,
            MAX(Properties) as Properties,
            MAX(StageInfo_Submitted) as StageInfo_Submitted,
            MAX(StageInfo_Completed) as StageInfo_Completed
          FROM (
            select
                Properties,
                `Stage Info`.`Stage ID` as ID,
                CASE WHEN event = 'SparkListenerStageSubmitted' THEN `Stage Info` ELSE NULL END as StageInfo_Submitted,
                CASE WHEN event = 'SparkListenerStageCompleted' THEN `Stage Info` ELSE NULL END as StageInfo_Completed
              from stages_raw
          ) GROUP BY ID
        )"""
    )

    self.df = spark.table('Stages')
    self.df_cached = self.df.cache()

  def display(self):
    self.df_cached.display()

  def get_stage_df(self, id):
    return self.df_cached.filter(col('id') == id)

  def get_longest_stage_id(self):
    return sql('select max_by(id, run_time) as id from stages where Failure_Reason is null').first()['id']

  def get_longest_stage(self):
    longest_stage_id = self.get_longest_stage_id()
    if longest_stage_id is None:
      return None
    return Stage(self, longest_stage_id)
  

# COMMAND ----------


