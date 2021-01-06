import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR, 
    gender VARCHAR, 
    itemInSession VARCHAR, 
    lastName VARCHAR, 
    length FLOAT, 
    level VARCHAR,
    location VARCHAR, 
    method VARCHAR, 
    page VARCHAR, 
    registration VARCHAR, 
    sessionId VARCHAR, 
    song VARCHAR, 
    status VARCHAR, 
    ts VARCHAR, 
    userAgent VARCHAR, 
    userId VARCHAR 
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR, 
    duration FLOAT,
    year INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) sortkey distkey PRIMARY KEY, 
    start_time TIMESTAMP NOT NULL REFERENCES time (start_time), 
    user_id VARCHAR NOT NULL REFERENCES users (user_id), 
    level VARCHAR, 
    song_id VARCHAR NOT NULL REFERENCES songs (song_id), 
    artist_id VARCHAR NOT NULL REFERENCES artists (artist_id), 
    session_id VARCHAR, 
    location VARCHAR, 
    user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR sortkey PRIMARY KEY, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR, 
    level VARCHAR)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR sortkey PRIMARY KEY,
    title VARCHAR, 
    artist_id VARCHAR, 
    year INT, 
    duration FLOAT)
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR sortkey PRIMARY KEY, 
    name VARCHAR, 
    location VARCHAR, 
    lattitude FLOAT, 
    longitude FLOAT)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP sortkey PRIMARY KEY,
    hour INT, 
    day INT, 
    week INT, 
    month INT,
    year INT, 
    weekday INT)
diststyle all;
""")

# COPY DATA FROM S3 TO STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2' compupdate off
    json 's3://udacity-dend/log_json_path.json';
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    compupdate off region 'us-west-2'
    json 'auto';
""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# INSERT RECORDS TO FACT AND DIMENTION TABLES FROM STAGINGN TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
)
    SELECT timestamp 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second' AS start_time, 
    se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
    FROM staging_events se
    INNER JOIN staging_songs ss
    ON se.song = ss.title 
    AND se.artist = ss.artist_name
    AND se.length = ss.duration
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (
    user_id, first_name, last_name, gender, level
) 
    SELECT DISTINCT userid, firstname, lastname, gender, level FROM staging_events;   
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id, title, artist_id, year, duration
)
    SELECT DISTINCT  song_id, title, artist_id, year, duration FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id, name, location, lattitude, longitude
) 
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,hour, day, week, month, year, weekday
)
    SELECT DISTINCT timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second' AS start_time,
    EXTRACT(h from start_time) AS hour,
    EXTRACT(d from start_time) AS day,
    EXTRACT(w from start_time) AS week,
    EXTRACT(mon from start_time) AS month,
    EXTRACT(y from start_time) AS year,
    EXTRACT(dayofweek from start_time) AS weekday
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
