#import pytest
from chispa.dataframe_comparer import * #https://github.com/MrPowers/chispa
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, DoubleType
from de_precompute_helper import *

def test_resample_hour(spark):
    input_data = [("tag1",'2022-01-01',1,1,10.0),("tag1",'2022-01-01',1,2,20.0),("tag1",'2022-01-01',1,3,30.0),("tag1",'2022-01-01',2,1,10.0),("tag1",'2022-01-01',2,2,20.0),("tag1",'2022-01-01',2,3,30.0),("tag1",'2022-01-02',1,1,10.0),("tag1",'2022-01-02',1,2,10.0),("tag1",'2022-01-02',1,3,10.0),("tag1",'2022-01-02',2,1,10.0),("tag1",'2022-01-02',2,2,20.0),("tag1",'2022-01-02',2,3,60.0),("tag2",'2022-01-01',1,1,100.0),("tag2",'2022-01-01',1,2,200.0),("tag2",'2022-01-01',1,3,300.0),("tag2",'2022-01-01',2,1,100.0),("tag2",'2022-01-01',2,2,200.0),("tag2",'2022-01-01',2,3,300.0),("tag2",'2022-01-02',1,1,100.0),("tag2",'2022-01-02',1,2,100.0),("tag2",'2022-01-02',1,3,100.0),("tag2",'2022-01-02',2,1,100.0),("tag2",'2022-01-02',2,2,200.0),("tag2",'2022-01-02',2,3,600.0)]
    
    input_schema = StructType([StructField('TagName', StringType(), True), StructField('date_part', StringType(), True), StructField('hour_part', IntegerType(), True), StructField('minute_part', IntegerType(), True), StructField('avg_value', DoubleType(), True)])
    
    input_data_df = spark.createDataFrame(input_data,input_schema)
    
    input_data_df = input_data_df.withColumn('date_part',to_date('date_part','yyyy-MM-dd'))
    
    expected_output = [("tag1",'2022-01-01',1,20.0),("tag1",'2022-01-01',2,20.0),("tag1",'2022-01-02',1,10.0),("tag1",'2022-01-02',2,30.0),("tag2",'2022-01-01',1,200.0),("tag2",'2022-01-01',2,200.0),("tag2",'2022-01-02',1,100.0),("tag2",'2022-01-02',2,300.0)]
    
    output_schema = StructType([StructField('TagName', StringType(), True), StructField('date_part', StringType(), True), StructField('hour_part', IntegerType(), True), StructField('hourly_avg', DoubleType(), True)])
    
    expected_output_df = spark.createDataFrame(expected_output,output_schema)
    
    expected_output_df = expected_output_df.withColumn('date_part',to_date('date_part','yyyy-MM-dd'))
    
    resampled_data_df = resample_hour(input_data_df,'2022-01-01','2022-01-03')
    
    assert_df_equality(resampled_data_df, expected_output_df,ignore_row_order=True) #it ignores the row ordering in two dataframes.
    

def test_interpolate_at_minute(spark):
    
    input_data = [("tag1",'2022-01-01 10:10:00.000',10.0),("tag1",'2022-01-01 10:11:00.000',12.0),("tag1",'2022-01-01 10:13:00.000',14.0),("tag1",'2022-01-01 10:16:00.000',20.0),("tag2",'2022-01-01 11:10:00.000',20.0),("tag2",'2022-01-01 11:11:00.000',22.0),("tag2",'2022-01-01 11:13:00.000',24.0),("tag2",'2022-01-01 11:16:00.000',30.0)]
        
    input_schema = StructType([StructField('TagName', StringType(), True), StructField('event_time_minute', StringType(), True),  StructField('Value', DoubleType(), True)])
    
    input_data_df = spark.createDataFrame(input_data,input_schema)
    
    input_data_df = input_data_df.withColumn('event_time_minute',to_timestamp('event_time_minute','yyyy-MM-dd HH:mm:ss.SSS'))
    
    expected_output = [("tag1",'2022-01-01 10:10:00.000',10.0),("tag1",'2022-01-01 10:11:00.000',12.0),("tag1",'2022-01-01 10:12:00.000',13.0),("tag1",'2022-01-01 10:13:00.000',14.0),("tag1",'2022-01-01 10:14:00.000',16.0),("tag1",'2022-01-01 10:15:00.000',18.0),("tag1",'2022-01-01 10:16:00.000',20.0),("tag2",'2022-01-01 11:10:00.000',20.0),("tag2",'2022-01-01 11:11:00.000',22.0),("tag2",'2022-01-01 11:12:00.000',23.0),("tag2",'2022-01-01 11:13:00.000',24.0),("tag2",'2022-01-01 11:14:00.000',26.0),("tag2",'2022-01-01 11:15:00.000',28.0),("tag2",'2022-01-01 11:16:00.000',30.0)]
    
    output_schema = StructType([StructField('TagName', StringType(), True), StructField('event_time_minute', StringType(), False),  StructField('Value', DoubleType(), True)])
    
    expected_output_df = spark.createDataFrame(expected_output,output_schema)
  
    expected_output_df = expected_output_df.withColumn('event_time_minute',to_timestamp('event_time_minute','yyyy-MM-dd HH:mm:ss.SSS'))

    interpolated_df = interpolate_at_minute(input_data_df)
    
    assert_df_equality(interpolated_df, expected_output_df,ignore_row_order=True,ignore_nullable=True) #it ignores the row ordering in two dataframes.
    
    
def test_transform_to_gold(spark):
    
    input_data = [("tag1",'2022-01-01 10:10:00.000',10.0),("tag1",'2022-01-01 10:11:00.000',12.0),("tag1",'2022-01-01 10:13:00.000',14.0),("tag2",'2022-01-01 11:10:00.000',20.0),("tag2",'2022-01-01 11:11:00.000',22.0)]
        
    input_schema = StructType([StructField('TagName', StringType(), True), StructField('event_time_minute', StringType(), False),  StructField('Value', DoubleType(), True)])
    
    input_data_df = spark.createDataFrame(input_data,input_schema)
    
    input_data_df = input_data_df.withColumn('event_time_minute',to_timestamp('event_time_minute','yyyy-MM-dd HH:mm:ss.SSS'))
    
    expected_output = [("tag1",'2022-01-01 10:10:00.000',10.0,'2022-01-01',10,10),("tag1",'2022-01-01 10:11:00.000',12.0,'2022-01-01',10,11),("tag1",'2022-01-01 10:13:00.000',14.0,'2022-01-01',10,13),("tag2",'2022-01-01 11:10:00.000',20.0,'2022-01-01',11,10),("tag2",'2022-01-01 11:11:00.000',22.0,'2022-01-01',11,11)]
    
    output_schema = StructType([StructField('TagName', StringType(), True), StructField('event_time_minute', StringType(), False),  StructField('avg_value', DoubleType(), True),StructField('date_part', StringType(), True), StructField('hour_part', IntegerType(), True), StructField('minute_part', IntegerType(), True)])
    
    expected_output_df = spark.createDataFrame(expected_output,output_schema)
  
    expected_output_df = expected_output_df.withColumn('event_time_minute',to_timestamp('event_time_minute','yyyy-MM-dd HH:mm:ss.SSS')).withColumn('date_part',to_date('date_part','yyyy-MM-dd'))

    actual_output_df = transform_to_gold(input_data_df)
    
    assert_df_equality(actual_output_df, expected_output_df,ignore_row_order=True,ignore_nullable=True) #it ignores the row ordering in two dataframes.
    
