# Databricks notebook source
#for unit testing
!pip install --upgrade pip && pip install chispa

# COMMAND ----------

!pip install --upgrade pip && pip install dbl-tempo

# COMMAND ----------

from test_de_precompute_helper import *

# COMMAND ----------

test_resample_hour(spark)

# COMMAND ----------

test_interpolate_at_minute(spark)

# COMMAND ----------

test_transform_to_gold(spark)

# COMMAND ----------


