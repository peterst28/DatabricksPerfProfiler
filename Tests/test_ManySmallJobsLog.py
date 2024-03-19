# Databricks notebook source
# MAGIC %run ../Includes/Includes

# COMMAND ----------

# MAGIC %sh mkdir -p /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/ManySmallJobs

# COMMAND ----------

# MAGIC %sh cp -r ./EventLogs/ManySmallJobs /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/

# COMMAND ----------

issue_manager = IssueManager()

# COMMAND ----------

event_log = EventLog('/Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/ManySmallJobs/*', True, '../Includes/Models/eventlog_schema.json')

# COMMAND ----------

EventLogChecker(event_log).check(issue_manager)

# COMMAND ----------

issue_manager.get_df().display()

# COMMAND ----------

issues = issue_manager.get_df().collect()

first_issue = issues[0]
assert(issue_manager.get_df().first()['Name'] == 'SmallJobs')
assert(issue_manager.get_df().first()['Severity'] == 'High')

# COMMAND ----------

# MAGIC %sh rm -r /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/ManySmallJobs/

# COMMAND ----------


