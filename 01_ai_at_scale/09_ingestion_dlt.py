# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from dlt_de_helper import *

# COMMAND ----------

spark.conf.set(
    "some Storage account key name",
    dbutils.secrets.get(scope="some-secrets-scope-name", key="some_key-name"))

spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

# COMMAND ----------

dbutils.widgets.text("path", "some/storage/account/path")
dbutils.widgets.text("time_gap_thresold", "12.948")#you ccan configure it

time_gap_thresold = float(dbutils.widgets.get("time_gap_thresold"))
files_path = dbutils.widgets.get("path")

# COMMAND ----------

@dlt.table
def tags_raw():
  return (
    spark.readStream.format("cloudFiles")
      .schema("TagName STRING, EventTime Timestamp, status STRING, value Float")
      .option("cloudFiles.format", "parquet")
      .option("cloudFiles.schemaEvolutionMode","none")
      .load(files_path)
  )

# COMMAND ----------

@dlt.table(
  name="tags_with_quarantine_data",
  temporary = True
)
def tags_with_quarantine_data():
    return (
        fetch_time_gap(dlt.readStream("tags_raw"))
    )

# COMMAND ----------

@dlt.table(
  name="quarantine_data"
)
def fetch_quarantine_data():
    return (
        quarantined_records_time_gap(dlt.readStream("tags_with_quarantine_data"),time_gap_thresold)
    )

# COMMAND ----------

@dlt.table(
  name="tags_bronze"
)
@dlt.expect("time_gap_too_far",f"time_gap_months <= {time_gap_thresold}")
def fetch_tags_bronze():
    return (
        fetch_time_details_hash(dlt.readStream("tags_with_quarantine_data"))
    )

dlt.create_streaming_live_table("tags_silver")

dlt.apply_changes(
  target = "tags_silver",
  source = "tags_bronze",
  keys = ["hash_diff"],
  sequence_by = col("EventTime"),
  except_column_list = ["time_gap_months"],
  stored_as_scd_type = 1
)
