# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * from hive_metastore.sternp.flights l
# MAGIC JOIN hive_metastore.sternp.flights as r ON l.id = r.id
# MAGIC where l.year < 1990 and r.year < 1990
