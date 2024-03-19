# Databricks notebook source
class JobFailuresChecker:

  def __init__(self, jobs):
    self.jobs = jobs

  def check(self, issue_manager):
    total_jobs = self.jobs.count()
    failed_jobs = self.jobs.count_failed()
    time_in_failed_jobs = self.jobs.get_failed_jobs_secs()
    total_runtime = self.jobs.get_total_runtime()
    # if time_in_failed_jobs is None:
    #   time_in_failed_jobs = 0
    # else:
    #   time_in_failed_jobs /= 1000
    prct_time_in_failed_jobs = time_in_failed_jobs / total_runtime
    job_failure_rate = failed_jobs / total_jobs

    if failed_jobs > 0:
      name = "JobFailures"
      description = f"You have {failed_jobs} job failures."
      description += "  Example failure: {}".format(self.jobs.get_failed_job_error())
      severity = "Medium"
      if job_failure_rate >= 1 or prct_time_in_failed_jobs >= 10:
        # more than 1% of jobs failed or 10% of time was spent in failed jobs
        severity = "High"
      time_lost_secs = time_in_failed_jobs
      recommended_resolution = "Fix the error."
      issue_manager.add_issue(name, description, severity, time_lost_secs, recommended_resolution)

# COMMAND ----------


