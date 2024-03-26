# DatabricksPerfProfiler

This repo is intended to look at a pipeline and determine if there are any performance / cost issues that can be addressed. It operates by looking at a cluster's eventlog, which captures a lot of information about what the cluster was doing.

## To Use

1. Configure [log delivery](https://docs.databricks.com/en/compute/configure.html#compute-log-delivery) on the cluster you'd like to diagnose so its eventlogs are delivered to DBFS.  Most of the time this will probably be a job cluster.
2. Run the pipeline you'd like to diagnose.
3. Clone this repo into your Databricks workspace.
4. Follow the instructions in the notebook `DBPerfProfiler`.  It will run against the logs you collected in steps 1 & 2.
