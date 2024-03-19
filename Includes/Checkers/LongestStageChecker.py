# Databricks notebook source
import builtins
import statistics

class LongestStageChecker:

  def __init__(self, stages, jobs, tasks, event_log, spark_env):
    self.stages = stages
    self.jobs = jobs
    self.tasks = tasks
    self.event_log = event_log
    self.spark_env = spark_env

  def check(self, issue_manager):

    longest_stage = self.stages.get_longest_stage()

    if longest_stage is None:
      return
    
    stage_run_time = longest_stage.get_run_time()

    # Don't bother checking the longest stage if it's less than 15 seconds
    if stage_run_time < 15:
      return
    
    total_runtime = self.jobs.get_total_runtime()
    prcnt_of_total_time = stage_run_time / total_runtime * 100
    if prcnt_of_total_time < 1:
      severity = "Low"
    elif prcnt_of_total_time < 5:
      severity = "Medium"
    else:
      severity = "High"

    job = self.jobs.get_job_from_stage(longest_stage.get_id())

    sql_execution = SQLExecution(longest_stage.get_SQL_ID(), self.event_log)
    sql_plan = sql_execution.get_sql_plan()
    sql_nodes = sql_plan.get_nodes_with_accumulators(longest_stage.get_accumulator_ids())
    physical_plan = sql_execution.get_physical_plan()
    notebook_path = job.get_notebook_path()
    snippet = job.get_description()

    ####################
    # Spill

    try:
      spill_bytes = longest_stage.get_disk_spill_bytes()

      if spill_bytes > 0:
        name = "Spill"
        description = f"Your longest stage ({longest_stage.get_id()}) is suffering from spill."
        time_lost_secs = stage_run_time 
        recommended_resolution = ""
        issue_manager.add_issue(name, description, severity, time_lost_secs, recommended_resolution, notebook_path, snippet, physical_plan)
    except PySparkValueError:
      print("No Spill")

    ####################
    # One Task

    if longest_stage.get_num_tasks() == 1:
      name = "OneTask"
      description = f"Your longest stage ({longest_stage.get_id()}) has only one task."
      time_lost_secs = stage_run_time 
      recommended_resolution = "This is usually caused by window functions with no partition, processing unsplittable files like gzip, or running an expensive UDF on small data."
      issue_manager.add_issue(name, description, severity, time_lost_secs, recommended_resolution, notebook_path, snippet, physical_plan)

    ####################
    # Skew

    longest_stage_id = longest_stage.get_id()
    task_run_times = self.tasks.get_task_runtimes(longest_stage_id)

    max_run_time = builtins.max(task_run_times)
    avg_run_time = statistics.mean(task_run_times)

    if max_run_time > 5 * avg_run_time:
      name = "Skew"
      description = f"Your longest stage ({longest_stage.get_id()}) has skew."
      time_lost_secs = stage_run_time 
      recommended_resolution = "This is usually the result of a skewed join."
      issue_manager.add_issue(name, description, severity, time_lost_secs, recommended_resolution, notebook_path, snippet, physical_plan)

    ####################
    # High Input

    bytes_read = longest_stage.get_bytes_read()
    cumulative_secs = longest_stage.get_cumulative_secs()

    # If the stage read more than 1 MB / second per core, consider this high input
    if bytes_read / cumulative_secs > 1000000:
      scan_node = [node for node in sql_nodes if node.is_scan_node()][0]
      scan_source = scan_node.get_scan_source()

      recommended_resolution = ""

      if scan_node.get_scan_source_type().lower().find('parquet') != -1:

        if not self.spark_env.using_photon():
          recommended_resolution += "- You're not using Photon.  Photon can accelerate reads from Parquet/Delta.\n"
          
        recommended_resolution += "- Consider if you can apply more filters or Liquid Partition this data.\n"

      name = "HighRead"
      description = f"Your longest stage ({longest_stage.get_id()}) is reading a lot of data ({bytes_read} bytes) from {scan_source}."
      time_lost_secs = stage_run_time
      issue_manager.add_issue(name, description, severity, time_lost_secs, recommended_resolution, notebook_path, snippet, physical_plan)

    ####################
    # High Output

    bytes_written = longest_stage.get_bytes_written()

    # If the stage wrote more than 1 MB / second per core, consider this high output
    if bytes_written / cumulative_secs > 1000000:

      recommended_resolution = ""

      if not self.spark_env.using_photon():
        recommended_resolution += "You're not using Photon.  Photon can accelerate writes to Parquet/Delta."

      name = "HighWrite"
      description = f"Your longest stage ({longest_stage.get_id()}) is writing a lot of data ({bytes_written} bytes)."
      time_lost_secs = stage_run_time 
      issue_manager.add_issue(name, description, severity, time_lost_secs, recommended_resolution, notebook_path, snippet, physical_plan)

    ####################
    # Shuffle

    shuffle_write = longest_stage.get_shuffle_bytes_written()
    shuffle_read = longest_stage.get_shuffle_bytes_read()

    # If the stage read or wrote more than 1 MB shuffle / second per core, consider this high shuffle
    if shuffle_write / cumulative_secs > 1000000 or shuffle_read / cumulative_secs > 1000000:
      name = "HighShuffle"
      description = f"Your longest stage ({longest_stage.get_id()}) is doing a lot of shuffle ({shuffle_read} bytes read and {shuffle_write} bytes written).  This is most likely due to an expensive join."
      time_lost_secs = stage_run_time 
      recommended_resolution = ""
      issue_manager.add_issue(name, description, severity, time_lost_secs, recommended_resolution, notebook_path, snippet, physical_plan)

    ####################
    # Small File Writes

    bytes_written = longest_stage.get_bytes_written()
    files_written = longest_stage.get_num_files_written()

    if bytes_written != 0 and files_written != 0:
      bytes_per_file = builtins.round(bytes_written / files_written)

      # if there are more than 100 files and avg file < 5MB
      if files_written > 100 and bytes_per_file < 5000000:
        name = "SmallFileWrite"
        description = f"Your longest stage ({longest_stage.get_id()}) is writing a lot of small files.  Avg file size: {bytes_per_file} bytes, Files written: {files_written}."
        time_lost_secs = stage_run_time 
        recommended_resolution = "If this table is partitioned, remove or adjust partitioning.  Consider setting the property 'delta.autoOptimize.optimizeWrite' to true on the destination table."
        issue_manager.add_issue(name, description, severity, time_lost_secs, recommended_resolution, notebook_path, snippet, physical_plan)

    ####################
    # Small File Reads
    row_groups_in_files = longest_stage.get_row_groups_to_read()
    total_files_size = longest_stage.get_file_size()

    avg_row_group_size = 0
    if total_files_size != 0:
      avg_row_group_size = total_files_size / row_groups_in_files 

    # if this stage read more than 100 row and avg row group < 5MB
    if row_groups_in_files > 100 and avg_row_group_size < 5000000:
      scan_node = [node for node in sql_nodes if node.is_scan_node()][0]
      scan_source = scan_node.get_scan_source()

      name = "SmallFileRead"
      description = f"Your longest stage ({longest_stage.get_id()}) is reading a lot of small files from {scan_source}"
      time_lost_secs = stage_run_time 
      recommended_resolution = f"If {scan_source} is partitioned, remove or adjust partitioning.  Try running OPTIMIZE on {scan_source}"
      issue_manager.add_issue(name, description, severity, time_lost_secs, recommended_resolution, notebook_path, snippet, physical_plan)

# COMMAND ----------


