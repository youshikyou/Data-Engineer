# Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

# Project Description
In this project, data modeling with Postgres and building an ETL pipeline. 


# State and justify your database schema design and ETL pipeline.
Two datasets:
    - log_data
    - song_data

One database:
    - Sparkify

Five tables: 
    - Songplay:    :fact table
    - user table   :dimension table
    - song table   :dimension table
    - artist table :dimension table
    - time table   :dimenstion table
    
songplay table
    - start_time 
    - user_id 
    - level 
    - song_id 
    - artist_id
    - session_id 
    - location 
    - user_agent
    

user table
    - user_id (PRIMARY KEY)
    - first_name 
    - last_name
    - gender
    - level

song table
    - song_id (PRIMARY KEY)
    - title
    - artist_id
    - year
    - duration

artist table
    - artist_id (PRIMARY KEY)
    - name
    - location
    - latitude
    - longitude

time table
    - start_time (PRIMARY KEY)
    - hour
    - day
    - week
    - month
    - year
    - weekday
    
ETL pipeline build:   
    process_data
        Iterating dataset to apply process_song_file and process_log_file functions
    process_song_file
        Process song dataset to insert record into songs and artists dimension table
    process_log_file
        Process log file to insert record into time and users dimensio table and songplays fact table    

# create_tables.py
creates the database and tables used to store the data.

# etl.ipynb
initially explore the data and test the ETL process.

# etl.py
reads in the Log and Song data files, processes and inserts data into the database.

# requirements.txt
A list of files used by this project.

# sql_queries.py
Defines all the SQL statements used by this project.

# test.ipynb
Test that data was loaded properly.
