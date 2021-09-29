import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col, to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('DEFAULT', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('DEFAULT', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Creates a spark entry point.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Processes song_data directory and creates songs and artists parquet files.

        Keyword arguments:
        spark -- a handle to spark session
        input_data -- S3 path to song_data 
        output_data -- S3 path to output directory 
    """
    
    # define a schema
    song_schema = "`num_songs` INT, `artist_id` STRING, `artist_latitude` DOUBLE, `artist_longitude` DOUBLE, `artist_location` STRING, `artist_name` STRING, `song_id` STRING, \
                   `title` STRING, `duration` DOUBLE, `year` INT"
    
    # get filepath to song data file
    song_data = input_data + "song-data/A/A/*/*.json"
        
    # read song data file
    df = spark.read.schema(song_schema).json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    songs_table.select('song_id','title','artist_id','year','duration').dropDuplicates().collect()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.format("parquet")\
      .mode("overwrite")\
      .partitionBy("year", "artist_id")\
      .option("compression", "snappy")\
      .save(output_data + "songs.parquet")
    
    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude',
                              'artist_longitude')
    artists_table.select('artist_id','artist_name','artist_location','artist_latitude', 'artist_longitude').dropDuplicates().collect()
    
    # write artists table to parquet files
    artists_table.write.format("parquet")\
      .mode("overwrite")\
      .option("compression", "snappy")\
      .save(output_data + "artists.parquet")

def process_log_data(spark, input_data, output_data):
    """Processes log_data directory and creates users, time, and songplays parquet files.

        Keyword arguments:
        spark -- a handle to spark session
        input_data -- S3 path to log_data 
        output_data -- S3 path to output directory 
    """
    
    #define a schema
    log_schema = "`artist` STRING, `auth` STRING, `firstName` STRING, `gender` STRING, `itemInSession` LONG, `lastName` STRING, `length` DOUBLE, `level` STRING, `location` STRING, \
                  `method` STRING, `page` STRING, `registration` DOUBLE, `sessionId` LONG, `song` STRING, `status` LONG, `ts` LONG, `userAgent` STRING, `userId` STRING"
    
    # get filepath to log data file
    log_data = input_data + 'log-data/*/*/*.json'

    # read log data file
    df = spark.read.schema(log_schema).json(log_data)
    
    # filter by actions for song plays
    df = df.filter(col("page") == 'NextSong')

    # extract columns for users table    
    users_table = df.select(col("userId").alias("user_id"),col("firstName").alias("first_name"),
                            col("lastName").alias("last_name"), 'gender', 'level')
    users_table.select('user_id','first_name','last_name','gender','level').dropDuplicates().collect()
    
    # write users table to parquet files
    users_table.write.format("parquet")\
      .mode("overwrite")\
      .option("compression", "snappy")\
      .save(output_data + "users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
        
    # create datetime column from original timestamp column
    # get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).datetime)
    # df = df.withColumn("datetime", get_datetime(col("ts")))
    
    # Converting ts to a timestamp format    
    time_table = df.withColumn('ts', to_timestamp(date_format((df.ts/1000).cast(dataType=TimestampType()), "yyyy-MM-dd HH:MM:ss z"), "yyyy-MM-dd HH:MM:ss z"))
        
    # extract columns to create time table    
    time_table = time_table.select(col("ts").alias("start_time"),
                                   hour(col("ts")).alias("hour"),
                                   dayofmonth(col("ts")).alias("day"), 
                                   weekofyear(col("ts")).alias("week"), 
                                   month(col("ts")).alias("month"),
                                   year(col("ts")).alias("year"),
                                   dayofweek(col("ts")).alias("weekday"))
    
        
    # write time table to parquet files partitioned by year and month
    time_table.write.format("parquet")\
      .mode("overwrite")\
      .partitionBy("year", "month")\
      .option("compression", "snappy")\
      .save(output_data + "time.parquet")

    # read in song data to use for songplays table
    song_data = input_data + "song-data/A/A/*/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(df, (df.artist == song_df.artist_name) & (df.song == song_df.title),'inner') \
        .withColumn("songplay_id", monotonically_increasing_id())\
        .withColumn('start_time', get_timestamp(df.ts))\
        .select("songplay_id",
           "start_time",                        
           col("userId").alias("user_id"),
           "level",
           "song_id",
           "artist_id",
           col("sessionId").alias("session_id"),
           col("artist_location").alias("location"),
           col("userAgent").alias("user_agent"),
           month(col("start_time")).alias("month"),
           year(col("start_time")).alias("year"))   

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.format("parquet")\
      .mode("overwrite")\
      .partitionBy("year", "month")\
      .option("compression", "snappy")\
      .save(output_data + "songsplay.parquet")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://shewit-udacity/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
