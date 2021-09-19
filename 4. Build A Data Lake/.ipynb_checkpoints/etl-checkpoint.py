import configparser
from datetime import datetime
import os
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


# Create Spark Session
def create_spark_session():
    """ 
        This function create a Spark Session instance to process data.
    """
        
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ 
        This function loads JSON input data (e.g. song_data) from the input_data path.
        The processed data will be loaded into song_table and artists_table specified by output_data.    
    """

    # get filepath to song data file
    song_data = input_data + 'song_data' + '/*/*/*/*.json'
    
    # read song data file
    df_songData = spark.read.json(song_data)
  
    
    #Check df_songData schema
    print("df_songData schema:")
    df_songData.printSchema() 
    
    
    # extract columns to create artists table
    artists_table = df_songData.select(artist_id,artist_name, artist_location, artist_latitude, artist_longitude)
    
    print("artists_table schema:")
    artists_table.printSchema()
    print("Top 5 lines in artists_table:")
    artists_table.show(5, truncate=False)
    
    
    # write artists table to parquet file
    artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')
    
    
    # extract columns to create songs table     
    songs_table = df_songData.select(song_id, title, artist_id, year, duration).dropDuplicates([song_id])
    
    print("songs_table schema:")
    songs_table.printSchema()
    print("Top 5 lines in songs_table:")
    songs_table.show(5, truncate=False)
    
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data, 'songs'), 'overwrite')

    
    

def process_log_data(spark, input_data, output_data):
    """
        This function reads log data from location specified in input_data, 
        transformed the data into fact and dimension tables (e.g time_table, users_table and songplays), 
        and store into Spark parquet files in the location specificed in output_data.
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data' + '/*/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("df.page=='NextSong'")

    # extract columns for users table    
    users_table = df.select(userId, firstName, lastName, gender, level).dropDuplicates().dropna()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0))

    df = df.withColumn('ts', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(x))
    df = df.withColumn('start_time', get_datetime(df.ts))

    
    
    # write time table to parquet files partitioned by year and month
    time_table = df.select('start_time') \
                    .withColumn('hour', hour(df.start_time)) \
                    .withColumn('day', dayofmonth(df.start_time)) \
                    .withColumn('week', weekofyear(df.start_time)) \
                    .withColumn('month', month(df.start_time)) \
                    .withColumn('year', year(df.start_time)) \
                    .withColumn('weekday', dayofweek(df.start_time))
    
    
                       
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time' ), 'overwrite')
    

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data' + '/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df \
                .join(song_df, \
                    (song_df.artist_name == df.artist) & \
                    (song_df.title == df.song) & \
                    (df.length ==song_df.duration)) \
                .withColumn('year', year(df.start_time)) \
                .withColumn('month', month(df.start_time)) \
                .withColumn('songplay_id', monotonically_increasing_id()) \
                .select('songplay_id', \
                        'start_time', \
                        df.userId.alias('user_id'), \
                        'level', \
                        'song_id', \
                        'artist_id', \
                        df.sessionId.cast(IntegerType()).alias('session_id'), \
                        'location', \
                        df.userAgent.alias('user_agent'), \
                        'year', \
                        'month') 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year' , 'month').parquet(os.path.join(output_data, 'songplays') , 'overwrite')

    
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3n://data-lake-sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
