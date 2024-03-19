# Databricks notebook source
class SQLExecution:

  def __init__(self, sql_id, event_log):
    self.event_log = event_log
    self.sql_id = sql_id
    self.raw_df = self.event_log.filter_events_df(
      ['org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate', 'org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart'])
    self.raw_df.createOrReplaceTempView('sql_plans_raw')

    self.sql_execution = (self.raw_df
                          .filter(col('executionId') == sql_id)
                          .filter("sparkPlanInfo:simpleString != 'AdaptiveSparkPlan isFinalPlan=false'")
                          .first()
                        )

  def get_sql_plan(self):
    return SQLPlan(self.sql_execution['sparkPlanInfo'])
  
  def get_physical_plan(self):
    return self.sql_execution['physicalPlanDescription']

# COMMAND ----------


