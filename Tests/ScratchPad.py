# Databricks notebook source
# MAGIC %run ../Includes/Includes

# COMMAND ----------

# MAGIC %sh mkdir -p /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/DataSkipping

# COMMAND ----------

# MAGIC %sh cp -r ./EventLogs/ReadManySmallFiles /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/

# COMMAND ----------

issue_manager = IssueManager()

# COMMAND ----------

event_log = EventLog('/Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/ReadManySmallFiles/*', True, '../Includes/Models/eventlog_schema.json')

# COMMAND ----------

jobs = Jobs(event_log)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jobs_raw where array_contains(`Stage IDs`, 3)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(event) from eventlog

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from eventlog where event in ('org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart')

# COMMAND ----------

spark_env = SparkEnv(event_log)

# COMMAND ----------

spark_env.using_photon()

# COMMAND ----------

stages = Stages(event_log)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stages where id = 1

# COMMAND ----------

1710766902687- 1710766892449

# COMMAND ----------

stage = stages.get_longest_stage()

# COMMAND ----------

stage.get_run_time()

# COMMAND ----------

jobs.get_total_runtime()

# COMMAND ----------

stages.get_longest_stage_id()

# COMMAND ----------

stage.get_row_groups_to_read()

# COMMAND ----------

stage.get_file_size()

# COMMAND ----------

3121818 / 2000 

# COMMAND ----------

print(stage.stage)

# COMMAND ----------

stage.get_id()

# COMMAND ----------

job = jobs.get_job_from_stage(stage.get_id())

# COMMAND ----------

job.get_description()

# COMMAND ----------

job.get_notebook_path()

# COMMAND ----------

builtins.round(stage.get_bytes_written() / 2000)

# COMMAND ----------

stage.df.display()

# COMMAND ----------

stage.get_accumulator_ids()

# COMMAND ----------

stage.get_id()

# COMMAND ----------

stage.get_SQL_ID()

# COMMAND ----------

sql_execution = SQLExecution(stage.get_SQL_ID(), event_log)

# COMMAND ----------

sql_execution.get_physical_plan()

# COMMAND ----------

sql_plan = sql_execution.get_sql_plan()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sql_plans_raw where executionId = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail delta.`abfss://unity@unitydemo.dfs.core.windows.net/b86c6879-8c55-4e70-a585-18d16a4fa6e9/tables/d26fe8fd-400d-44de-9e31-2570167afb80`

# COMMAND ----------

sql_nodes = sql_plan.get_nodes_with_accumulators(stage.get_accumulator_ids())

# COMMAND ----------

sql_nodes[0].get_scan_source()

# COMMAND ----------

print(sql_nodes[2])

# COMMAND ----------

print(sql_plan)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from eventlog where Event = 'SparkListenerStageCompleted'

# COMMAND ----------

spark.table('Executors').select('`Executor Info`.`Total Cores`').collect()[0]['Total Cores']

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view stages_test as
# MAGIC   select `Task Info`.`Task ID` as ID, `Stage ID` as StageID, `Stage Attempt ID` as StageAttemptID, `Task Metrics`.`Executor Run Time` as Run_Time, * from EventLog where Event in ('SparkListenerTaskEnd')

# COMMAND ----------

import builtins
import statistics

rows = spark.table('stages_test').filter(col('StageID') == 2).select('Run_time').collect()
run_times = [row['Run_time'] / 1000 for row in rows]
max_run_time = builtins.max(run_times)
avg_run_time = statistics.mean(run_times)

print(max_run_time)
print(avg_run_time)

# COMMAND ----------

stages_df = event_log.filter_events_df(['SparkListenerStageSubmitted', 'SparkListenerStageCompleted'])
stages_df.createOrReplaceTempView('stages_raw')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stages_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from Stages

# COMMAND ----------

EventLogChecker(event_log).check(issue_manager)

# COMMAND ----------

issue_manager.get_df().display()

# COMMAND ----------

assert(issue_manager.get_df().isEmpty())

# COMMAND ----------

# MAGIC %sh rm -r /Volumes/sternp/default/volume/tmp/dbprofiler/Eventlogs/Skew/

# COMMAND ----------


