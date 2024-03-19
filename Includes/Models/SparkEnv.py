# Databricks notebook source
class SparkEnv:

  def __init__(self, event_log):
    self.event_log = event_log
    self.df = self.event_log.filter_events_df(['SparkListenerEnvironmentUpdate'])
    self.df_cached = self.df.cache()

  def display(self):
    self.df_cached.display()

  def get_cloud_provider(self):
    return self.df_cached.select('`Spark Properties`.`spark.databricks.clusterUsageTags.cloudProvider`').collect()[0]['spark.databricks.clusterUsageTags.cloudProvider']
  
  def get_dbr_name(self):
    return self.df_cached.select('`Spark Properties`.`spark.databricks.clusterUsageTags.effectiveSparkVersion`').collect()[0]['spark.databricks.clusterUsageTags.effectiveSparkVersion']

  def get_dbr_version(self):
    dbr_name = self.get_dbr_name()
    return dbr_name[0:dbr_name.find('-')]
  
  def using_photon(self):
    dbr_name = self.get_dbr_name()
    return dbr_name.find('photon') != -1
  
  def get_driver_type(self):
    return self.df_cached.select('`Spark Properties`.`spark.databricks.driverNodeTypeId`').collect()[0]['spark.databricks.driverNodeTypeId']
  
  def get_worker_type(self):
    return self.df_cached.select('`Spark Properties`.`spark.databricks.workerNodeTypeId`').collect()[0]['spark.databricks.workerNodeTypeId']
  
  def using_spot(self):
    return float(self.df_cached.select('`Spark Properties`.`spark.databricks.clusterUsageTags.clusterSpotBidMaxPrice`').collect()[0]['spark.databricks.clusterUsageTags.clusterSpotBidMaxPrice']) != -1.0
  
  def get_cluster_type(self):
    return self.df_cached.select('`Spark Properties`.`spark.databricks.clusterSource`').collect()[0]['spark.databricks.clusterSource']
  
  def is_job_cluster(self):
    return self.get_cluster_type() == 'JOB'
  
  def get_num_workers(self):
    return self.df_cached.select('`Spark Properties`.`spark.databricks.clusterUsageTags.clusterTargetWorkers`').collect()[0]['spark.databricks.clusterUsageTags.clusterTargetWorkers']
