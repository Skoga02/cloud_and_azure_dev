# Databricks notebook source
# MAGIC %md
# MAGIC # EDA on supply chain

# COMMAND ----------

# TODO: list the volume
VOLUME_PATH = "/Volumes/supply_chain_demo/default/raw"

spark.sql(f"LIST '{VOLUME_PATH}'")