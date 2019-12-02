import configparser
from datetime import datetime
import os
import io
import zipfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, unix_timestamp
from pyspark.sql import types as t
import pandas as pd


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    df.createOrReplaceTempView("song_data")
    # extract columns to create songs table
    songs_table = spark.sql("""
    select song_id, title, artist_id, year, duration from song_data
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(output_data+"songs")

    df.createOrReplaceTempView("artist_data")
    # extract columns to create artists table
    artists_table = spark.sql("""
    select artist_id, artist_name, artist_location, artist_latitude, artist_longitude from artist_data
    """)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+"artists")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log-data/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df[df['page']=='NextSong']

    df.createOrReplaceTempView("user_data")
    # extract columns for users table    
    users_table = spark.sql("""
    select userId, firstName, lastName, gender, level from user_data
    """)
    
    # write users table to parquet files
    users_table.write.parquet(output_data+"users")

    # create timestamp column from original timestamp column 
    # create datetime column from original timestamp column
    
    df = df.withColumn('start_time', date_format((df.ts/1000.0).cast(dataType=t.TimestampType()), "yyyy-MM-dd HH:mm:ss"))
    
    df = df.withColumn("hour", hour(df['start_time']))
    df = df.withColumn("day",dayofmonth(df['start_time']) )
    df = df.withColumn("week",weekofyear(df['start_time']))
    df = df.withColumn("month",month(df['start_time']))
    df = df.withColumn("year",year(df['start_time']))
    
    df.createOrReplaceTempView("time_data")
    
    # extract columns to create time table
    time_table = spark.sql("""
    select start_time, hour, day, week, month, year  from time_data
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(output_data+"time")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    song_df.createOrReplaceTempView("song_table")
    
    
    
    df = df.withColumn("songplay_id",monotonically_increasing_id())
    df.createOrReplaceTempView("songplays_table")
    
    
     # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    select spt.songplay_id, spt.start_time, spt.userId, spt.level, spt.sessionId, spt.location, spt.userAgent,
    spt.year, spt.month, spt.length, st.song_id, st.artist_id, st.title, st.artist_name, st.duration
    from songplays_table as spt join song_table as st
    on spt.artist = st.artist_name
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data+"songplays")


def main():
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    
    #input_data = ""
    #output_data = ""
    
    with zipfile.ZipFile('song-data.zip') as zf:
    zf.extractall()
    
    with zipfile.ZipFile('log-data.zip') as zf:
    zf.extractall('log-data')

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
