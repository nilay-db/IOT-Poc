# Databricks notebook source
!pip install --upgrade pip && pip install dbl-tempo

# COMMAND ----------

from de_precompute_helper import *

# COMMAND ----------

silve_to_gold(spark,"hive_metastore.shell_dev_db.tags_silver","hive_metastore.shell_dev_db.tags_gold",'2020-12-01 01:00:00.000','2023-03-28 12:00:00.000',"overwrite")

# COMMAND ----------


