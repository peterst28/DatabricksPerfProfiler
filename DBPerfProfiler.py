# Databricks notebook source
log_location = '/path/to/log'

# COMMAND ----------

# MAGIC %run ./Includes/Includes

# COMMAND ----------

#%sh cp -r /databricks/driver/eventlogs/* /dbfs/tmp/dbprofiler/

# COMMAND ----------

# MAGIC %sh mkdir -p /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/Basic

# COMMAND ----------

# MAGIC %sh cp -r Tests/EventLogs/Basic /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/

# COMMAND ----------

# MAGIC %fs ls /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/Basic/

# COMMAND ----------

issue_manager = IssueManager()

# COMMAND ----------

event_log = EventLog('/Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/Basic/*', True, './Includes/Models/eventlog_schema.json')
# event_log.display()

# COMMAND ----------

EventLogChecker(event_log).check(issue_manager)

# COMMAND ----------

issue_manager.get_df().display()

# COMMAND ----------


