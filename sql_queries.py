# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id SERIAL,
    start_time TIMESTAMP REFERENCES time(start_time),
    user_id INTEGER REFERENCES users(user_id),
    level VARCHAR(8),
    song_id VARCHAR(18) REFERENCES songs(song_id),
    artist_id VARCHAR(255) REFERENCES artists(artist_id),
    session_id VARCHAR(255),
    location VARCHAR(255),
    user_agent VARCHAR(255)
);

""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(63),
    level VARCHAR(63)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(18) PRIMARY KEY ,
    title VARCHAR(255),
    artist_id VARCHAR(255),
    year SMALLINT,
    duration NUMERIC(11,5)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(18) PRIMARY KEY, 
    name VARCHAR(255),
    location VARCHAR(255), 
    latitude NUMERIC(8,5), 
    longitude NUMERIC(19,15)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY,
    hour SMALLINT,
    day SMALLINT,
    week SMALLINT,
    month SMALLINT,
    year SMALLINT,
    weekday SMALLINT
);
""")



# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id,level,song_id,artist_id,session_id,location, user_agent)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT
DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
VALUES(%s,%s,%s,%s,%s)
ON CONFLICT (user_id)
DO
UPDATE
  SET
first_name = %s,
last_name = %s,
gender = %s,
level = %s;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s);
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s);
""")


time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
values(%s, %s, %s, %s, %s, %s, %s);
""")

# FIND SONGS

song_select = ("""
SELECT song_id, a.artist_id, s.title, s.duration, a.name 
FROM 
    songs AS s JOIN artists AS a
    ON s.artist_id = a.artist_id
WHERE title = %s AND name = %s AND duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]