# Databricks notebook source
# MAGIC %run ../Includes/Includes

# COMMAND ----------

# MAGIC %sh mkdir -p /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/ExecutorFail

# COMMAND ----------

# MAGIC %sh cp -r ./EventLogs/ExecutorFail /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/

# COMMAND ----------

issue_manager = IssueManager()

# COMMAND ----------

event_log = EventLog('/Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/ExecutorFail/*', True, '../Includes/Models/eventlog_schema.json')

# COMMAND ----------

EventLogChecker(event_log).check(issue_manager)

# COMMAND ----------

issue_manager.get_df().display()

# COMMAND ----------

assert(issue_manager.get_df().count() == 2)

issues = issue_manager.get_df().collect()

first_issue = issues[0]
assert(issue_manager.get_df().first()['Name'] == 'JobFailures')
assert(issue_manager.get_df().first()['Severity'] == 'High')
assert(issue_manager.get_df().first()['Time_Lost_Secs'] == 55.836)

second_issue = issues[1]
assert(second_issue['Name'] == 'ExecutorFailures')
assert(second_issue['Severity'] == 'High')

# COMMAND ----------

# MAGIC %sh rm -r /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/ExecutorFail/

# COMMAND ----------


