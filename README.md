# Data Lake Project
One Paragraph of project description goes here

## Getting Started

These instructions will allow you to read S3 data, transform it into useful tables, and write the tables back to s3

### Prerequisites

Python, an Amazon AWS account with an S3 bucket, an EMR cluster, and IAM credentials for testing locally

```
The code provided works when ssh'd into an EMR cluster, the main changes to run locally would be to include an AWS access key id and an AWS secret access key
```

## Running the program

etl_emr.py - run this script once you have ssh'd into your AWS EMR cluster as 'python etl_emr.py'.  For this project I used four m5.xlarge marchines in the us-west2 region


### Details behind the program

create_spark_session function - creates a distributed spark session
process_song_data function
1.  retrieves all song files from the song_data folder (and nested folders) located in an s3 bucket
2.  songs_table contains the following fields:  song_id (unique string), title (string), artist_id (string), year (int), duration (double).  table is written to s3 bucket in folder 'songs' partitioned by year and artist_id
3.  artists_table contains the following fields:  artist_id (unique string), name (string), location (string), latitude (double), longitude (double).  table is written to s3 bucket in folder 'artists'
process_log_data function
1.  retrieves all songplay log files from the log_data folder (and nested folders) located in an s3 bucket
2.  users_table contains the following fields:  user_id (unique string), first_name (string), last_name (string), gender (string), level (string).  table is written to s3 bucket in folder 'users'
3.  timestamp and datetime fields are derived from the 'ts' column in the log data
4.  time_table contains the following fields:  start_time (string), hour (integer), day (integer), week (integer), month (integer), year (integer), weekday (string).  table is written to s3 bucket in folder 'time' partitioned by year and month 
5.  songplays_table references the song files and ties them to the songplay logs, fields include: songplay_id (counter long), start_time (string), user_id (string), level (string), song_id (string), artist_id (string), session_id (long), location (string), user_agent (string).  table is written to s3 bucket in folder 'songplays' partitioned by year and month


### Examples queries from generated parquet tables

Explain what these tests test and why

```
Give an example
```


## Built With

* python
* pyspark

## Authors

* **Adrian Brown** 
