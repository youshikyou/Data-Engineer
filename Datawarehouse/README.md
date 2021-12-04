The purpose of this project:
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, 
in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
Building an ETL pipeline that extracts their data from S3, stages them in Redshift, 
and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

Project Description
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift.
To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.


The analytical goals:
Test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


Idea of this project:
    1.Complete the sql_queries.py: including create the staging table, the fact table and dimension tables.
    2.Create the myredshift IAM role.
    3.Attach this role to the created redshift cluster, which means to create a redshift security group in EC2
    3.create the redshift cluster
    4.copy staging table, create fact table and dimention tables.
    5.ETL data into the fact table and dimention tables.
    6.test the tables (query in the redshift)

Database schema:

Fact Table
    songplays:records in event data associated with song plays i.e. records with page NextSong
          - songplay_id
          - start_time
          - user_id
          - level
          - song_id
          - artist_id
          - session_id
          - location
          - user_agent
          
Dimension Tables
    users:users in the app
          - user_id
          - first_name
          - last_name
          - gender
          - level
    
    
   songs:songs in music database
          - song_id
          - title
          - artist_id
          - year
          - duration
   
   
   artists:artists in music database
          - artist_id
          - name
          - location
          - lattitude
          - longitude
          
          
   time:timestamps of records in songplays broken down into specific units
          - start_time
          - hour
          - day
          - week
          - month
          - year
          - weekday


ETL pipeline:
    copy the song dataset and log dataset from s3 to redshift as the staging datasets
    using the staging datasets to load the fact table and the other dimension tables.
    


