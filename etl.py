import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import  year, month,dayofweek,  dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
import  pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create a spark session"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Read song data from source json files ,extract songs and artist tables then store the in parqute files in the target location
    Parameters:
    spark: spark session
    input_data: source of songs json files
    output_data: target to store extracted tables in as parquet files.
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # create song data schema 
    from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date,TimestampType as Ts
    songSchema = R([
        Fld("song_id",Str()),
        Fld("title",Str()),
        Fld("duration",Dbl()),
        Fld("year",Int()),
        Fld("artist_id",Str()),
        Fld("artist_name",Str()),
        Fld("artist_latitude",Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_location",Dbl()),
        Fld("num_songs",Int()),
    ])
    
    # read song data file
    df = spark.read.json(song_data,schema=songSchema)

    # define fields to be created in the extracted songs_table
    songs_table_fields =["song_id","title","artist_id","year","duration"]
    
    # extract columns to create songs table
    songs_table = df.select(songs_table_fields).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(output_data + 'songs/')

    # define artist table fields
    artists_table_fields = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    
    # extract columns to create artists table
    artists_table = df.select(artists_table_fields).dropDuplicates();
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/')

    
def process_log_data(spark, input_data, output_data):
    """Read song log data from source json files ,extract time and users tables then store them in parqute files in the target location, 
    read songs and artist table from previously saved parquet files  and join the result with the extracted data from the song log
    save the join result (songs_plays) in parequet file in the target location
    Parameters:
    spark: spark session
    input_data: source of songs json files
    output_data: target to store extracted tables in as parquet files.
    """
    # get filepath to log data file
    log_data = input_data + 'log-data/*/*/*.json'
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    
    # define fields for user table
    users_table_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    
    # extract columns for users table    
    users_table = df.selectExpr(users_table_fields).dropDuplicates()
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x // 1000).replace(microsecond=x % 1000),TimestampType())
    df = df.withColumn("time", get_timestamp('ts'))
    
    # extract columns to create time table
    time_table = df.select("time","ts").dropDuplicates().withColumn("hour", hour(col("time"))).\
    withColumn("day", dayofmonth(col("time"))).withColumn("week", dayofweek(col("time"))).\
    withColumn("month", month(col("time"))).withColumn("year", year(col("time"))).\
    withColumn("weekday", dayofweek(col("time"))).select('ts','time','hour', 'day', 'week', 'month', 'year', 'weekday')
   
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/')
    
    # read in song data to use for songplays table
    songs_table = spark.read.parquet(output_data + 'songs/*/*/*')
    
    # read in artist data to use for songplays table
    artist_table= spark.read.parquet(output_data + 'artists/*')

    # rename time in time_table to prevent column duplicated when joining with songs table
    time_table = time_table.selectExpr("ts as playtime,time as dftime","hour","day","week","month","year",
    "weekday" ) 

    # define columns for songs_plays table
    songs_plays_table_fields = ["userId as user_id","level","song_id","artist_id","playtime","sessionId as session_id", "useragent as user_agent","year","month"]
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(songs_table, [df.song == songs_table.title,df.length ==songs_table.duration]).join(time_table,df.ts ==time_table.playtime).join(artist_table,df.artist == artist_table.artist_name).selectExpr(songs_plays_table_fields).dropDuplicates()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + 'songplays/')

    
def main():
    """
    create spark session
    set input/output data source and target buckets
    call process_song_data function
    call process_log_data function
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    
    # set the target bucket
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
