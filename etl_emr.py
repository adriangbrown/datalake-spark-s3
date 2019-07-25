from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
        
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """Function extracts s3 song log files, creates songs and 
       artists dimensional tables and writes them back to s3 """
    
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    song_df = spark.read.json(song_data)

    songs_table = song_df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    songs_table = songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'), \
      mode='overwrite')

    artists_table = song_df.select('artist_id', col('artist_name').alias('name'), \
      col('artist_location').alias('location'), col('artist_latitude').alias('latitude'), \
      col('artist_longitude').alias('longitude')).dropDuplicates()
    artists_table = artists_table.write.parquet(os.path.join(output_data, 'artists'), mode='overwrite')

def process_log_data(spark, input_data, output_data):
    """Function extracts s3 songplay log files, creates users, time, song dimensional tables, and 
       sonplays fact table and writes them all back to s3 """
    
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')
    log_df = spark.read.json(log_data)    
    log_df = log_df.filter(log_df.page=='NextSong')
    log_df = log_df.withColumn('songplay_id', monotonically_increasing_id())

    users_table = log_df.select(col('userId').alias('user_id'), col('firstName').alias('first_name'), \
    col('lastName').alias('last_name'), 'gender', 'level').dropDuplicates()
    users_table = users_table.write.parquet(os.path.join(output_data, 'users'), mode='overwrite')

    get_timestamp = udf(lambda ms: datetime.fromtimestamp(ms/1000).strftime('%H:%M:%S'))
    log_df = log_df.withColumn('start_time', get_timestamp(col('ts')))
    get_datetime = udf(lambda ms: datetime.fromtimestamp(ms/1000).strftime('%Y-%m-%d %H:%M:%S'))
    log_df = log_df.withColumn('datetime', get_datetime(col('ts')).cast('timestamp'))
    log_df = log_df.withColumn('month', month(col('datetime')))
    
    time_table = log_df.select(('start_time'), \
                 hour('datetime').alias('hour'), \
                 dayofmonth('datetime').alias('day'), \
                 weekofyear('datetime').alias('week'), \
                 month('datetime').alias('month'), \
                 year('datetime').alias('year'), \
                 date_format('datetime', 'u').alias('weekday'))
    time_table = time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, \
      'time'), mode='overwrite')
    
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    song_df = spark.read.json(song_data)
    songplays_table = log_df.join(song_df, (log_df.song==song_df.title) & \
      (log_df.artist==song_df.artist_name) & (log_df.length==song_df.duration))

    songplays_table = songplays_table.select('songplay_id', 'start_time', col('userId').alias('user_id'), \
      'level', 'song_id', 'artist_id', col('sessionId').alias('session_id'), 'location', \
      col('userAgent').alias('user_agent'), 'month', 'year').write.partitionBy('year', \
      'month').parquet(os.path.join(output_data, 'songplays'), mode='overwrite')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://adrian-s3-bucket2/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
