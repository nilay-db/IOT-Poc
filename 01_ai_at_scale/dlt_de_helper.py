#from dlt import *
from pyspark.sql.functions import *

#This function accept a dataframe with raw tags data and returns a DF with timestamp converted to unix timestamp, current timestamp and years of gap between event time and present time.
def fetch_time_gap(tag_df):
    return tag_df.withColumn("event_time_unix", unix_timestamp("EventTime")) \
            .withColumn("current_time",unix_timestamp()) \
            .withColumn("time_gap_months",(abs(col("current_time")-col("event_time_unix")))/2592000)

#This function fetches errored record and return that dataframe, this dataframe can be saved as table.
def quarantined_records_time_gap(df,time_gap_thresold):
    return df.filter(f"time_gap_months > {time_gap_thresold}")

def fetch_time_details_hash(tags_df):
    return tags_df \
            .withColumn("event_time_minute",date_trunc("minute",col("EventTime"))) \
            .fillna(0.0, "value") \
            .withColumn("hash_diff",sha1(concat("TagName","EventTime","value"))) \
            .withColumn("current_time",from_unixtime(col("current_time"))) \
            .drop("event_time_unix","_rescued_data")
