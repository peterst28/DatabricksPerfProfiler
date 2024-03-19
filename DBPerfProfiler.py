# Databricks notebook source
event_log_location = 'dbfs:/cluster-logs/pete-test/0319-144731-5w8yvgzj/eventlog/0319-144731-5w8yvgzj_10_139_64_178/3068293179564032684/'

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Performance Profiler
# MAGIC
# MAGIC This notebook is intended to look at a pipeline and determine if there are any performance / cost issues that can be addressed.  It operates by looking at a cluster's eventlog, which captures a lot of information about what the cluster was doing.
# MAGIC
# MAGIC ## Set up
# MAGIC
# MAGIC 1. Configure [log delivery](https://docs.databricks.com/en/compute/configure.html#compute-log-delivery) on the cluster you'd like to diagnose so its eventlogs are delivered to DBFS.  Most of the time this will probably be a job cluster.
# MAGIC 1. Run the pipeline you'd like to diagnose.
# MAGIC 1. Find the path to the eventlogs on DBFS and set it in the Python variable `event_log_location` above.  It will likely be a path similar to `dbfs:/cluster-logs/pete-test/0319-144731-5w8yvgzj/eventlog/0319-144731-5w8yvgzj_10_139_64_178/3068293179564032684/`.  In this case, I had set the cluster delivery path to `dbfs:/cluster-logs/pete-test/`.  The rest of the path is generated.  If this path is not set correctly, you will get an error in the next cell.
# MAGIC 1. Run the whole notebook.
# MAGIC 1. Look at the list of issues displayed in the last cell.

# COMMAND ----------

spark.read.json(event_log_location)

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


