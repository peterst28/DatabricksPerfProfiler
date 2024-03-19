# Databricks notebook source
# MAGIC %run ../Includes/Includes

# COMMAND ----------

# MAGIC %sh mkdir -p /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/OneTask

# COMMAND ----------

# MAGIC %sh cp -r ./EventLogs/OneTask /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/

# COMMAND ----------

issue_manager = IssueManager()

# COMMAND ----------

event_log = EventLog('/Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/OneTask/*', True, '../Includes/Models/eventlog_schema.json')

# COMMAND ----------

EventLogChecker(event_log).check(issue_manager)

# COMMAND ----------

jobs = Jobs(event_log)
jobs.display()

# COMMAND ----------

issue_manager.get_df().display()

# COMMAND ----------

assert(issue_manager.get_df().count() == 2)

issues = issue_manager.get_df().collect()

first_issue = issues[0]
assert(first_issue['Name'] == 'Spill')
assert(first_issue['Severity'] == 'Low')
assert(first_issue['Time_Lost_Secs'] == 49.399)

second_issue = issues[1]
assert(second_issue['Name'] == 'OneTask')
assert(second_issue['Severity'] == 'Low')
assert(second_issue['Time_Lost_Secs'] == 49.399)

# COMMAND ----------

# MAGIC %sh rm -r /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/OneTask/

# COMMAND ----------


