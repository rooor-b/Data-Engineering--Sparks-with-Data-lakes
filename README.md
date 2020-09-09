# Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app..

As their data engineer, I built an ETL pipeline pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.
# Project Description
In this project,I apply what I've learned on on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. I load data from S3, process the data into analytics tables using Spark, and load them back into S3.

# Project Datasets
Here are the S3 links for sparkify database:

* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data


# Project Steps
* Create a Spark session

* Read song data from s3://udacity-dend/song_data bucket then extract song and artist dimentional tables

* Store song and artist data in the target bucket as parquet files

* Read log data from s3://udacity-dend/log_data bucket then extract time and users dimentional tables

* Store users and time data in the target bucket as parquet files

* Read songs from parquet file and join them with time_table and log data table to extract songs_plays Fact tabel

* Store songs_plays fact table in the target bucket as paraquet file

# How To Run

To succefully run this project follow the steps in the comand line:
*  set your AWS credintial in dl.cfg file
*  run *python etl.py*

Alternativly you can deploy etl.py file to EMR cluster
