#import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek,  monotonically_increasing_id
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']

def songSchema():
    """
    Creates a schema for song records
    """

    return StructType(
            [
                StructField('num_songs', IntegerType(), True),
                StructField('artist_id', StringType(), True),
                StructField('artist_latitude', StringType(), True),
                StructField('artist_longitude', StringType(), True),
                StructField('artist_location', StringType(), True),
                StructField('artist_name', StringType(), True),
                StructField('song_id', StringType(), True),
                StructField('title', StringType(), True),
                StructField('duration', DoubleType(), True),
                StructField('year', IntegerType(), True),
                ]
            )

def logSchema():
    """
    Creates a schema for song records
    """

    return StructType(
            [
                StructField('artist', StringType(), True),
                StructField('auth', StringType(), True),
                StructField('firstName', StringType(), True),
                StructField('gender', StringType(), True),
                StructField('itemInSession', IntegerType(), True),
                StructField('lastName', StringType(), True),
                StructField('length', DoubleType(), True),
                StructField('level', StringType(), True),
                StructField('location', StringType(), True),
                StructField('method', StringType(), True),
                StructField('page', StringType(), True),
                StructField('registration', LongType(), True),
                StructField('sessionId', IntegerType(), True),
                StructField('song', StringType(), True),
                StructField('status', IntegerType(), True),
                StructField('ts', LongType(), True),
                StructField('userAgent', StringType(), True),
                StructField('userId', IntegerType(), True),
                ]
            )

def create_spark_session():
    """
    Get or create a Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):
    """
    process_song_data:
    function to process song data
    args:
        spark: Spark object session
        input_data: filepath where the data is stored
        output_data: filepath where to store the processed data
    """
    # get filepath to song data file
    song_data =  input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.schema(songSchema()).json(song_data) 

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year',
            'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite') .partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'parquet/songs'))

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_latitude',
            'artist_longitude', 'artist_location',
            'artist_name').dropDuplicates() 
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'parquet/artists'))



def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "data/log_data/*.json"

    # read log data file
    df = spark.read.schema(logSchema()).json(log_data) 
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong") 

    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level").dropDuplicates(["user_id"])
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data,
        'parquet/users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x/1000.0)), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col('ts')))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp((x/1000.0)), DateType())
    df = df.withColumn("datetime", get_datetime(col('ts'))) 
    
    # extract columns to create time table
    time_table = df.selectExpr("timestamp as start_time", "hour(timestamp) as hour",
    "dayofmonth(timestamp) as day",
    "weekofyear(timestamp) as week",
    "month(timestamp) as month",
    "year(timestamp) as year",
    "dayofweek(timestamp) as weekday").dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy('year',
            'month').parquet(os.path.join(output_data, 'parquet/time'))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'parquet/songs'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title,
            'inner').selectExpr("timestamp as start_time", "userId as user_id", "level", "song_id", "artist_id", "sessionId as session_id", "location", "userAgent as user_agent", "month(timestamp) as month", "year(timestamp) as year").withColumn("songplay_id", monotonically_increasing_id()).dropDuplicates(["start_time"])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy('year',
            'month').parquet(os.path.join(output_data, 'parquet/songplays'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-test-sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
