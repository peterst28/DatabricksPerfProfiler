# Databricks notebook source
class Jobs:

  def __init__(self, event_log):
    self.event_log = event_log
    self.raw_df = self.event_log.filter_events_df(['SparkListenerJobEnd', 'SparkListenerJobStart'])
    self.raw_df.createOrReplaceTempView('jobs_raw')

    sql("""CREATE OR REPLACE TEMP VIEW Jobs AS
        SELECT
        *,
        Completion_Time - Submission_Time AS Run_Time
        FROM
        (
          select
            `Job ID` as ID,
            MAX(`Completion Time`) AS Completion_Time,
            MAX(`Job Result`.Result) AS Result,
            MAX(`Job Result`.Exception.Message) AS Exception,
            MAX(`Submission Time`) AS Submission_Time,
            MAX(Properties.`spark.databricks.notebook.path`) AS Notebook_Path,
            MAX(Properties.`spark.job.description`) AS Description,
            MAX(`Stage IDs`) AS Stage_IDs
          from jobs_raw
          GROUP BY `Job ID`
        )"""
    )

    self.df = spark.table('Jobs')
    self.df_cached = self.df.cache()

  def display(self):
    self.df_cached.display()

  def get_job_from_stage(self, stage_id):
    return Job(self.df_cached.filter(array_contains(col('Stage_IDs'), stage_id)).first())

  def get_df(self):
    return self.df_cached
  
  def count(self):
    return self.df_cached.count()

  # from start of 1st job to end of last
  def get_total_runtime(self):
    return sql('select MAX(Completion_Time) - MIN (Submission_Time) AS RunTime FROM Jobs').collect()[0]['RunTime']/1000

  def get_job_stats(self):
    return self.df_cached.select(max(col('run_time')).alias('max'), avg(col('run_time')).alias('avg')).collect()[0]

  def get_avg_job_length(self):
    return self.get_job_stats()['avg'] / 1000

  def get_max_job_length(self):
    return self.get_job_stats()['max'] / 1000

  def count_failed(self):
    return self.df_cached.filter(col('Result') == 'JobFailed').count()

  def get_failed_jobs_secs(self):
    secs =  sql('select sum(Completion_Time - Submission_Time) As Time_Secs from jobs where Result = "JobFailed"').collect()[0]["Time_Secs"]
    if secs is None:
      return 0
    return secs / 1000
  
  def get_failed_job_error(self):
    return self.df_cached.filter(col('Exception').isNotNull()).select('Exception').first()[0]

# COMMAND ----------


