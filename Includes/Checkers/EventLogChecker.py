# Databricks notebook source
class EventLogChecker:
  
  def __init__(self, event_log):
    self.event_log = event_log

  def check(self, issue_manager):

    jobs = Jobs(self.event_log)
    job_gaps = JobGaps(jobs)
    spark_env = SparkEnv(event_log)
    executor_removals = ExecutorRemovals(event_log)
    stages = Stages(event_log)
    tasks = Tasks(event_log)

    JobGapsChecker(spark_env, jobs, job_gaps).check(issue_manager)
    JobFailuresChecker(jobs).check(issue_manager)
    ExecutorRemovalChecker(executor_removals).check(issue_manager)
    ManySmallJobsChecker(jobs).check(issue_manager)
    LongestStageChecker(stages, jobs, tasks, event_log, spark_env).check(issue_manager)
