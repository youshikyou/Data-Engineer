import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= (""" 
    CREATE TABLE IF NOT EXISTS staging_events(
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR(1),
        itemInSession INT,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location TEXT,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        sessionId INT,
        song VARCHAR,
        status VARCHAR,
        ts BIGINT,
        userAgent TEXT,
        userId INT
    )
""")

staging_songs_table_create=("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location TEXT,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INT
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay(
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        userId INT NOT NULL,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        sessionId INT,
        location TEXT,
        userAgent TEXT    
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        userId INT,
        first_name VARCHAR,
        last_name VARCHAR,
        gender CHAR(1),
        level VARCHAR
    )
""")



song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song(  
        song_id VARCHAR,
        title VARCHAR,
        artist_id VARCHAR,
        year INT,
        duration FLOAT    
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist(
        artist_id VARCHAR,
        name VARCHAR,
        location TEXT,
        latitude FLOAT,
        longitude FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
        credentials 'aws_iam_role=arn:aws:iam::587146224181:role/myRedshiftRole'
        region 'us-west-2'
        format as json {}
""").format(LOG_DATA,LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role=arn:aws:iam::587146224181:role/myRedshiftRole'
    region 'us-west-2'
    json 'auto'
""").format(SONG_DATA)

# FINAL TABLES 

songplay_table_insert = ("""
    INSERT INTO songplay(start_time, userId, level, song_id, artist_id, sessionId, location, userAgent)
    SELECT DISTINCT 
        TIMESTAMP with time zone 'epoch' + (se.ts / 1000) * INTERVAL '1 second' AS start_time,
        se.userId AS userId,
        se.level AS level,
        ss.song_id AS song_id,
        ss.artist_id AS artist_id,
        se.sessionId AS sessionId,
        se.location AS location,
        se.userAgent AS userAgent
    FROM
        staging_events AS se
    INNER JOIN
        staging_songs AS ss
    ON 
        se.song = ss.title AND
        se.artist = ss.artist_name AND
        se.length= ss.duration
    WHERE
        se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users(userId, first_name,last_name,gender,level)
    SELECT DISTINCT
        se.userId AS userId,
        se.firstName AS first_name,
        se.lastName AS last_name,
        se.gender AS gender,
        se.level AS level
    FROM 
        staging_events AS se
        
    WHERE
        se.userId IS NOT NULL AND page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO song (song_id,title,artist_id,year,duration)
    SELECT DISTINCT
        ss.song_id AS song_id,
        ss.title AS title,
        ss.artist_id AS artist_id,
        ss.year AS year,
        ss.duration AS duration
    FROM
        staging_songs AS ss
    WHERE
        ss.song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artist(artist_id, name,location,latitude,longitude)
    SELECT DISTINCT
        ss.artist_id AS artist_id,
        ss.artist_name AS name,
        ss.artist_location AS location,
        ss.artist_latitude AS latitude,
        ss.artist_longitude AS longitude
    FROM
        staging_songs AS ss
    WHERE
        ss.artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time(start_time,hour,day,week,month,year,weekday)
    SELECT DISTINCT
           start_time,
           extract(hour from start_time), 
           extract(day from start_time), 
           extract(week from start_time), 
           extract(month from start_time), 
           extract(year from start_time), 
           extract(weekday from start_time)
    FROM
        songplay
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
