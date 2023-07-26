#for unit testing
#!pip install --upgrade pip && pip install chispa

#import pytest
from chispa.dataframe_comparer import * #https://github.com/MrPowers/chispa
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, DoubleType, FloatType, LongType

from dlt_de_helper import * # importing the library for which unit test case is created

def test_fetch_time_details_hash(spark):
    input_data = [("xxyy1",'2022-01-05T06:26:59.387+0000','Good',22.0,1641364019,1679673132,15.0),("xxyy2",'2022-01-21T17:28:40.070+0000','Good',21.0,1642786120,1679673132,15.0),("xxyy3",'2022-01-10T01:53:19.582+0000','Good',20.0,1641779599,1679673132,15.0)]
    
    input_schema = StructType([StructField('TagName', StringType(), True), StructField('EventTime', StringType(), True), StructField('status', StringType(), True), StructField('value', FloatType(), True), StructField('event_time_unix', LongType(), True), StructField('current_time', LongType(), True), StructField('time_gap_months', DoubleType(), True)])
    
    input_data_df = spark.createDataFrame(input_data,input_schema)
    
    input_data_df = input_data_df.withColumn('EventTime',to_timestamp('EventTime'))
    
    expected_output = [("xxyy1",'2022-01-05T06:26:59.387+0000','Good',22.0,'2023-03-24 15:52:12',15.0,'2022-01-05T06:26:00.000+0000'),("xxyy2",'2022-01-21T17:28:40.070+0000','Good',21.0,'2023-03-24 15:52:12',15.0,'2022-01-21T17:28:00.000+0000'),("xxyy3",'2022-01-10T01:53:19.582+0000','Good',20.0,'2023-03-24 15:52:12',15.0,'2022-01-10T01:53:00.000+0000')]
    
    output_schema = StructType([StructField('TagName', StringType(), True), StructField('EventTime', StringType(), True), StructField('status', StringType(), True), StructField('value', FloatType(), False), StructField('current_time', StringType(), True),StructField('time_gap_months', DoubleType(), True), StructField('event_time_minute', StringType(), True)])
    
    expected_output_df = spark.createDataFrame(expected_output,output_schema)
    expected_output_df = expected_output_df.withColumn("EventTime",to_timestamp("EventTime")).withColumn("event_time_minute",to_timestamp("event_time_minute")).withColumn("hash_diff",sha1(concat("TagName","EventTime","value")))
    
    actual_output_df = fetch_time_details_hash(input_data_df)
    
    assert_df_equality(actual_output_df, expected_output_df,ignore_row_order=True)


    
def test_quarantined_records_time_gap(spark):
    
    time_gap_thresold = 19.0
    
    input_data = [("xxyy1",'2022-01-05T06:26:59.387+0000','Good',22.0,1641364019,1679673132,20.0),("xxyy2",'2022-01-21T17:28:40.070+0000','Good',21.0,1642786120,1679673132,18.0),("xxyy3",'2022-01-10T01:53:19.582+0000','Good',20.0,1641779599,1679673132,15.0)]
    
    input_schema = StructType([StructField('TagName', StringType(), True), StructField('EventTime', StringType(), True), StructField('status', StringType(), True), StructField('value', FloatType(), True), StructField('event_time_unix', LongType(), True), StructField('current_time', LongType(), True), StructField('time_gap_months', DoubleType(), True)])
    
    input_data_df = spark.createDataFrame(input_data,input_schema)
    
    input_data_df = input_data_df.withColumn('EventTime',to_timestamp('EventTime'))
    
    expected_data = [("xxyy1",'2022-01-05T06:26:59.387+0000','Good',22.0,1641364019,1679673132,20.0)]
    
    expected_data_df = spark.createDataFrame(expected_data,input_schema).withColumn('EventTime',to_timestamp('EventTime'))
    
    actual_output_df = quarantined_records_time_gap(input_data_df,time_gap_thresold)
    
    assert_df_equality(actual_output_df, expected_data_df,ignore_row_order=True)
    
    
