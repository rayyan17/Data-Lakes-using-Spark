# Sparkify DataStorage
Our client Sparkify is growing exponentially and has moved most of its data into S3 storage. Now the is residing in various formats there. It is best to move from a traditional DataWarehouse to a Data Lake. Now we need to build a pipeline that can read data from S3, convert the data into a Dimensional Table and store it back into another S3 storage.

## S3 Storage
1. Input Data:
Our input data is residing in the following storage: "s3a://udacity-sparkify-rk/"

2. Output Data:
We will be stroing our Dimensional Tables in Parquet files at the location: "s3a://udacity-sparkify-output/"


## Schema Design
Star Schema is used to build this database.
Songplays table is used as a Fact table and other tables users, songs, artists and time were acting as Dimension Tables expalining detailed information about each fact.


## ETL Pipeline (etl.py)
### Extraction:
We extract data for songs and related logs from the following directories:
```
s3://udacity-dend/song_data
s3://udacity-dend/log_data
```


### Transformation
From songs data we have transformed our data in 2 different tables: songs and artists table
- songs table: songs in music database (song_id, title, artist_id, year, duration)
- artists table: artists in music database (artist_id, name, location, lattitude, longitude)
 
From log_data we have transformed our data to fit in time and users data
- users table: users in the app (user_id, first_name, last_name, gender, level)
- time table:  timestamps of records in songplays broken down into specific units (start_time, hour, day, week, month, year, weekday)

Information from log_data and other tables were used to build the songsplays table
- songplays table: records in log data associated with song plays i.e. records with page NextSong (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)


### Load
All the data from directories is transferred to S3 storage (s3a://udacity-sparkify-output/). All the tables will be stored as Parquet files.


## Running the project
In order to run the project from the scratch run the following commands from your terminal:

```
python3.6 etl.py
```

Remember, to add the AWS credentials in the configuration files to access the S3 bucket storage.
