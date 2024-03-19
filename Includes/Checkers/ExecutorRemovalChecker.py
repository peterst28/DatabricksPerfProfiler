# Databricks notebook source
class ExecutorRemovalChecker:

  def __init__(self, executor_removals):
    self.executor_removals = executor_removals

  def check(self, issue_manager):
    executor_fails_count = self.executor_removals.count_executor_fails()

    if executor_fails_count > 0:
      name = "ExecutorFailures"
      description = f"You have {executor_fails_count} executor failures.  These kinds of failures are most commonly caused by memory issues."
      executor_fails_reason = self.executor_removals.get_executor_fail_error()
      description += f"  Example failure: {executor_fails_reason}"
      severity = "High"
      time_lost_secs = None
      recommended_resolution = "Try increasing the amount of memory on your workers without increasing the number of cores (usually a different class of instance)."
      issue_manager.add_issue(name, description, severity, time_lost_secs, recommended_resolution)

# COMMAND ----------


