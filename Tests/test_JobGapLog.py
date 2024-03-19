# Databricks notebook source
# MAGIC %run ../Includes/Includes

# COMMAND ----------

# MAGIC %sh mkdir -p /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/JobGaps

# COMMAND ----------

# MAGIC %sh cp -r ./EventLogs/JobGaps /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/

# COMMAND ----------

issue_manager = IssueManager()

# COMMAND ----------

event_log = EventLog('/Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/JobGaps/*', True, '../Includes/Models/eventlog_schema.json')

# COMMAND ----------

EventLogChecker(event_log).check(issue_manager)

# COMMAND ----------

issue_manager.get_df().display()

# COMMAND ----------

assert(issue_manager.get_df().count() == 1)
assert(issue_manager.get_df().first()['Name'] == 'JobGaps')
assert(issue_manager.get_df().first()['Severity'] == 'High')
assert(issue_manager.get_df().first()['Time_Lost_Secs'] == 37.362)

# COMMAND ----------

# MAGIC %sh rm -r /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/JobGaps/

# COMMAND ----------


