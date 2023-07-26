# Databricks notebook source
# MAGIC %md
# MAGIC This notebook is created to Run unit test cases for dlt_de_helper, After we package the code this notebook may not be required.

# COMMAND ----------

#for unit testing
!pip install --upgrade pip && pip install chispa

# COMMAND ----------

from dlt_de_helper import * # importing the library for which unit test case is created
from test_dlt_de_helper import * # importing the  unit test case library

# COMMAND ----------

test_fetch_time_details_hash(spark)

# COMMAND ----------

test_quarantined_records_time_gap(spark)
