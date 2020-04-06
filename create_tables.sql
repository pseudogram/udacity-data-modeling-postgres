
-- Create The Database
DROP DATABASE IF EXISTS sparkifydb;
CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0;
GRANT ALL PRIVILEGES ON DATABASE sparkifydb TO dattlee; postgres=> \list

-- Songplay_table_create =
DROP TABLE IF EXISTS songplays;
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id UUID PRIMARY KEY,
    start_time TIMESTAMP REFERENCES time(start_time),
    user_id UUID REFERENCES users(user_id),
    level varchar(8),
    song_id UUID REFERENCES songs(song_id),
    artist_id UUID REFERENCES artists(artist_id),
    session_id UUID,
    location VARCHAR(255),
    user_agent VARCHAR(255)
);



-- user_table_create 
CREATE TABLE IF NOT EXISTS users(
    user_id UUID PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(255),
    level VARCHAR(8)
)


-- song_table_create
DROP TABLE IF EXISTS songs;
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(18) PRIMARY KEY ,
    title VARCHAR(255),
    artist_id VARCHAR(255),
    year SMALLINT,
    duration NUMERIC(11,5) -- MAX OF 24HR IN SECONDS
)

-- song_table insert
INSERT INTO customer_transactions (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s);

-- time_table_create
CREATE TABLE IF NOT EXISTS artists(
    artist_id UUID PRIMARY KEY, 
    name VARCHAR(255),
    location VARCHAR(255), 
    latitude VARCHAR(255), 
    longitude VARCHAR(255)
)


-- time_table_create
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER CONSTRAINT max_hour CHECK (hour < 24),
    day INTEGER CONSTRAINT max_day CHECK (hour < 32),
    week INTEGER CONSTRAINT max_week CHECK (hour < 53),
    month INTEGER CONSTRAINT max_month CHECK (hour < 13),
    year INTEGER,
    weekday BOOLEAN
)




-- --------------
-- INSERT RECORDS
-- --------------


songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]