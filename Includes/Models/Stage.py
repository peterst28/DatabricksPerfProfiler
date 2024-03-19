# Databricks notebook source
class Stage:

  def __init__(self, stages, id):
    self.stages = stages
    self.df = self.stages.get_stage_df(id)
    self.stage = self.df.first()
    self.id = id

  def display(self):
    self.df.display()

  def get_run_time(self):
    return self.stage['Run_Time'] / 1000
  
  def get_disk_spill_bytes(self):
    spill = self.stage['Disk_Bytes_Spill']
    if spill is None:
      return 0
    return int(spill)
  
  # ID of the SQL Query that kicked off this stage
  def get_SQL_ID(self):
    sql_id = self.stage['SQL_ID']
    if sql_id is None:
      return None
    return int(sql_id)
  
  def get_bytes_read(self):
    bytes_read = self.stage['BytesRead']
    if bytes_read is None:
      return 0
    return int(bytes_read)
  
  def get_num_files_written(self):
    files_written = self.stage['NumFilesWritten']
    if files_written is None:
      return 0
    return int(files_written)
  
  # I believe this is the total size of the files read
  def get_file_size(self):
    file_size = self.stage['TotalRowGroupsSize']
    if file_size is None:
      return 0
    return int(file_size)

  def get_row_groups_to_read(self):
    row_groups = self.stage['RowGroupsToRead']
    if row_groups is None:
      return 0
    return int(row_groups)

  def get_bytes_written(self):
    bytes_written = self.stage['BytesWritten']
    if bytes_written is None:
      return 0
    return int(bytes_written)
  
  def get_shuffle_bytes_written(self):
    shuffle_bytes_written = self.stage['ShuffleBytesWritten']
    if shuffle_bytes_written is None:
      return 0
    return int(shuffle_bytes_written)
  
  def get_shuffle_bytes_read(self):
    shuffle_bytes_read = self.stage['ShuffeBytesRead']
    if shuffle_bytes_read is None:
      return 0
    return int(shuffle_bytes_read)

  def get_id(self):
    return self.id
  
  def get_num_tasks(self):
    return self.stage['Num_Tasks']
  
  def get_num_workers(self):
    return int(self.stage['Properties']['spark.databricks.clusterUsageTags.clusterWorkers'])
  
  # I believe this is the
  # cumulative seconds the stage spent across all tasks
  def get_cumulative_secs(self):
    return float(self.stage['Executor_Time']) / 1000
  
  def get_accumulator_ids(self):
    accumulators = self.df.selectExpr('explode(Accumulables.ID) as id').collect()
    return [row['id'] for row in accumulators]

