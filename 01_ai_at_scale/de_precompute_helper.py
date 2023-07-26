# This has all the business logic and helper functions to perform precomute transformations. Unit test cases for these function are in other file.
from pyspark.sql.functions import *
from tempo import *

#Assumes that silver layer base table has data at minutes freq for each tag,This base table should be populated after applying interpolation for absent events.
def resample_hour(input_df, start_date, end_date):
    output_df = input_df.filter(f"date_part >= '{start_date}' AND date_part < '{end_date}'").groupBy("TagName","date_part","hour_part").avg("avg_value").withColumnRenamed("avg(avg_value)","hourly_avg")
    return output_df

#df with TagName, Event_time_in_minute and value fields
def interpolate_at_minute(source_df):
    input_tsdf = TSDF( source_df,partition_cols=["TagName"],ts_col="event_time_minute")
    interpolated_tsdf = input_tsdf.resample(freq="1 minute", func="mean").interpolate(method="linear")
    output_spark_df = interpolated_tsdf.df
    return output_spark_df

def transform_to_gold(interpolated_df):
    return interpolated_df.withColumn("date_part",to_date(col("event_time_minute"))) \
.withColumn("hour_part",hour(col("event_time_minute"))) \
.withColumn("minute_part",minute(col("event_time_minute"))) \
.withColumnRenamed("Value","avg_value")


def silve_to_gold(spark,source_table, target_table, event_start_timestamp, event_end_timestamp,write_mode):
    #source_table - name of source table from silver table, this dataset should be resampled to minute and interpolated
    #target_table- target table in gold layer where this data should be written to
    #event_start_timestamp- event start time,in case of incremental processing
    #event_end_timestamp- event start time,in case of incremental processing
    #write_mode - to 'append'/'overwrite' to target table

    source_df = spark.read.table(source_table).filter(f"EventTime >= to_timestamp('{event_start_timestamp}','yyyy-MM-dd HH:mm:ss.SSS') and EventTime < to_timestamp('{event_end_timestamp}','yyyy-MM-dd HH:mm:ss.SSS')")
    source_df = source_df.select("TagName","event_time_minute","Value")
    interpolated_df = interpolate_at_minute(source_df)
    gold_df = transform_to_gold(interpolated_df)
    gold_df.write.mode(write_mode).saveAsTable(target_table)
    
#Input_df is based on data read from tags_gold table, resample_period_unit can be minute or hour or day, resample_period can be any number
def dynamic_resample(input_df,resample_period, resample_period_unit,start_date,end_date):
    
    filtered_df = input_df.filter(f"date_part >= '{start_date}' AND date_part < '{end_date}'")
    
    if resample_period_unit == 'minute':
        
        resampled_df = filtered_df.withColumn("minute_part", floor(col("minute_part")/ resample_period) * resample_period ).groupBy("TagName","date_part","hour_part","minute_part").avg("avg_value").withColumn("date_time",concat(col("date_part"),lit("T"),lpad(col("hour_part"),2,"0"),lit(":"),lpad(col("minute_part"),2,"0"),lit(":00.000"))).withColumnRenamed("avg(avg_value)","avg_value").select( "TagName","date_time","avg_value")

    elif resample_period_unit == 'hour':
        
        resampled_df = filtered_df.withColumn("hour_part", floor(col("hour_part") / resample_period) * resample_period).groupBy("TagName","date_part","hour_part").avg("avg_value").withColumn("date_time",concat(col("date_part"),lit("T"),lpad(col("hour_part"),2,"0"),lit(":00:00.000"))).withColumnRenamed("avg(avg_value)","avg_value").select( "TagName","date_time","avg_value")
    
    else:
        
        resampled_df = filtered_df.groupBy("TagName","date_part").avg("avg_value").withColumn("date_time",concat(col("date_part"),lit("T"),lit("00:00:00.000"))).withColumnRenamed("avg(avg_value)","avg_value").select( "TagName","date_time","avg_value")

    return resampled_df
