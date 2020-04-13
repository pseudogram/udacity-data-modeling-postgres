import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *
from datetime import datetime

pd.set_option('mode.chained_assignment', None)


def process_song_file(cur, filepath):
    """Given the path for a song data file, extract data for song and artist tables and load it into database.

    Args:
        - cur: cursor pointing to database
        - filepath: path to song data
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]

    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Given the path for a log data file, extract data for time, user and songplay tables and load it into database.

    Args:
        - cur: cursor pointing to database
        - filepath: path to song data
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = df['ts'].apply(lambda x: datetime.fromtimestamp(x/1000))
    
    # insert time data records
    time_data = []
    for item in t:
        row = [
            item.strftime("%Y-%m-%d %H:%M:%S"),
            item.hour,
            item.day,
            item.isocalendar()[1], # week of year
            item.month,
            item.year,
            item.weekday()
        ]
        time_data.append(row)

    column_labels = ['timestamp','hour','day','week','month','year','weekday']
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level','ts']]
    user_df['userId'] = user_df['userId'].astype('int')
    user_df.sort_values(by=['userId','ts'], inplace=True)  # sort by timestamp so old user data is updated
    user_df.drop(columns=['ts'], inplace=True)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, np.concatenate((row.values, row[1:].values)))

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        row_time = datetime.fromtimestamp(row.ts/1000).strftime("%Y-%m-%d %H:%M:%S")
        songplay_data = (row_time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Given a path directory, traverse all subdirectories and process any files using ``func``.

    Args:
        - cur: cursor pointing to database
        - conn: connection to database
        - filepath: directory containing containg data fo the same format
        - func: function to process each file
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    """When executing this file, this function runs processing all files under directories *data/song_data* and 
    *data/log_data*."""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    conn.set_session(autocommit=True)

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
