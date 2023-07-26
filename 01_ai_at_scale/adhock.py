# Databricks notebook source
!pip install --upgrade pip && pip install chispa

# COMMAND ----------

!pip install --upgrade pip && pip install dbl-tempo

# COMMAND ----------

from pyspark.sql.functions import *
from dlt_de_helper import *
from test_dlt_de_helper import *
from de_precompute_helper import *
#from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, DoubleType, FloatType, LongType

# COMMAND ----------

test_fetch_time_details_hash(spark)

# COMMAND ----------

gold_df = spark.read.table("hive_metastore.shell_dev_db.tags_gold")

# COMMAND ----------

resampled_df = dynamic_resample(gold_df,2,'hour','2021-01-01','2021-01-02')

# COMMAND ----------

display(resampled_df.orderBy("TagName","EventTime"))

# COMMAND ----------

silver_df = spark.read.table("hive_metastore.shell_dev_db.tags_silver")

# COMMAND ----------

    
    event_start_timestamp = '2021-01-01 01:00:00.000'
    event_end_timestamp = '2021-01-05 12:00:00.000'
    source_table = "hive_metastore.shell_dev_db.tags_silver"
    source_df = spark.read.table(source_table).filter(f"EventTime >= to_timestamp('{event_start_timestamp}','yyyy-MM-dd HH:mm:ss.SSS') and EventTime < to_timestamp('{event_end_timestamp}','yyyy-MM-dd HH:mm:ss.SSS')")
    source_df = source_df.select("TagName","event_time_minute","Value")
    interpolated_df = interpolate_at_minute(source_df)
    gold_df = transform_to_gold(interpolated_df)


# COMMAND ----------

display(source_df.orderBy("TagName","event_time_minute"))

# COMMAND ----------

display(gold_df.orderBy("TagName","event_time_minute"))

# COMMAND ----------


