# Databricks notebook source
event_log_location = 'dbfs:/cluster-logs/pete-test/0319-144731-5w8yvgzj/eventlog/0319-144731-5w8yvgzj_10_139_64_178/3068293179564032684/'

# COMMAND ----------

# MAGIC %run ./Includes/Includes

# COMMAND ----------

issue_manager = IssueManager()

# COMMAND ----------

event_log = EventLog(event_log_location, True, './Includes/Models/eventlog_schema.json')

# COMMAND ----------

EventLogChecker(event_log).check(issue_manager)

# COMMAND ----------

issue_manager.get_df().display()

# COMMAND ----------


