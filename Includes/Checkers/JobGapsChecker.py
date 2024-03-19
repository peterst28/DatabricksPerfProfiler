# Databricks notebook source
class JobGapsChecker:

  def __init__(self, spark_env, jobs, job_gaps):
    self.jobs = jobs
    self.job_gaps = job_gaps
    self.spark_env = spark_env

  def check(self, issue_manager):

    max_job_gap_secs = self.job_gaps.get_max_job_gap_secs()
    total_job_gap_secs = self.job_gaps.get_total_job_gap_secs()

    if max_job_gap_secs > 30:
      name = "JobGaps"
      description = "You have gaps between jobs.  The gaps may be due to running non-Spark code, orchestration, or driver issues."
      if not self.spark_env.is_job_cluster():
        description += "  It looks like you're running an interactive cluster, so the gaps could also be due to time between query submissions."
      
      gap_percent_time = total_job_gap_secs / self.jobs.get_total_runtime() * 100
      severity = "Low"
      if gap_percent_time >= 10:
        # more than 10% of time is in the gaps
        severity = "High"
      elif gap_percent_time >= 1:
        severity = "Medium"
      time_lost_secs = total_job_gap_secs
      recommended_resolution = "Find out what's running in the gaps.  Write non-Spark code to use Spark or try increasing driver size."
      issue_manager.add_issue(name, description, severity, time_lost_secs, recommended_resolution)
