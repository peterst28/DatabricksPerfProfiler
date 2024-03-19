# Databricks notebook source
class ManySmallJobsChecker:

  def __init__(self, jobs):
    self.jobs = jobs

  def check(self, issue_manager):
    avg_job_length = self.jobs.get_avg_job_length()
    max_job_length = self.jobs.get_max_job_length()
    total_runtime = self.jobs.get_total_runtime()

    max_job_percent = max_job_length / total_runtime * 100

    # If the max job is less than 30 seconds or less than 5% of the overall time
    # TODO: Is this the best way to find a small jobs problem?
    if max_job_length < 30 or max_job_percent < 5:
      name = "SmallJobs"
      description = "You have a lot of small jobs. In cases like this, the overhead of running jobs may be high compared to the actual execution time, and you may not be fully utilizing your cluster."
      severity = "High"
      time_lost_secs = None
      recommended_resolution = "Use Delta Live Tables or run small queries in parallel using Python ThreadPools"
      issue_manager.add_issue(name, description, severity, time_lost_secs, recommended_resolution)

# COMMAND ----------


