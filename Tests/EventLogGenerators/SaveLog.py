# Databricks notebook source
import shutil
import os

# COMMAND ----------

src_root = "/databricks/driver/eventlogs"
src = f"{src_root}/{os.listdir(src_root)[0]}/"
src

# COMMAND ----------

dbutils.widgets.text("destination", "./EventLogs/FillMeIn")
dest = dbutils.widgets.get("destination")
dest

# COMMAND ----------

print(f"Copying files from {src} to {dest}")
shutil.copytree(src, dest)
