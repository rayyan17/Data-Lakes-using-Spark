import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config["AWS"]['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config["AWS"]['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create Spark Session
    
    Return (obj): Spark configured session instance
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process song data files and store results for songs and artists table in S3
    Args:
        spark (obj): spark instance
        input_data (str): path to read data of log files from Storage
        output_data (str): path to store tables parquet file in S3 Storage
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs.parquet", mode="overwrite")

    # extract columns to create artists table
    artist_columns = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]
    artists_table = df.selectExpr(*artist_columns)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """Process log data files and store results for users, time, songplay table in S3
    Args:
        spark (obj): spark instance
        input_data (str): path to read data of log files from Storage
        output_data (str): path to store tables parquet file in S3 Storage
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # extract columns for users table    
    user_cols = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    user_table = df.selectExpr(*user_cols).dropDuplicates()
    
    # write users table to parquet files
    user_table.write.parquet(output_data + "users.parquet", mode="overwrite")

    # create timestamp column fromdf original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0))
    df = df.withColumn("time_format", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    time_cols = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    time_funcs = [udf(lambda x: x.isoformat()), udf(lambda x: x.hour),
                  udf(lambda x: x.day), udf(lambda x: x.date().isocalendar()[1]), 
                  udf(lambda x: x.month), udf(lambda x: x.year), udf(lambda x: x.weekday())]
    
    for col_val, col_func in zip(time_cols, time_funcs):
        df = df.withColumn(col_val, col_func(df.time_format))
    
    # extract columns to create time table
    time_table = df.select(time_cols)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time.parquet", mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs.parquet")
    song_df = song_df.join(spark.read.parquet(output_data + "artists.parquet").select(["artist_id", "name"]), on=["artist_id"], how='inner')

    # extract columns from joined song and log datasets to create songplays table 
    songplay_cols_expr = ["songplay_id", "start_time", "userId as user_id", "level", "song_id", "artist_id", "sessionId as session_id", "location", "userAgent as user_agent"]
    join_conds = [df.song == song_df.title, df.artist == song_df.name, df.length == song_df.duration]
    songplays_table = df.join(song_df, join_conds, how="left").withColumn("songplay_id", monotonically_increasing_id()).selectExpr(*songplay_cols_expr)


    # write songplays table to parquet files 
    songplays_table.write.parquet(output_data + "songplays.parquet", mode="overwrite")


def main():
    """Project Entry Points"""
    spark = create_spark_session()
    input_data = "s3a://udacity-sparkify-rk/"
    output_data = "s3a://udacity-sparkify-output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

