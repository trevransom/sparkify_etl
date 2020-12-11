import os
from io import StringIO
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def check_data_quality(df):
    """
    - Ensures the checked df has 1 row
    - Returns True if df has 1 row, else False
    """

    if df.shape[0] == 1:
        return True


def optimize_insert(cur, df, table_name, table_insert):
    """
    - Tries to use the efficient copy_from method
    - If there's a unique primary key violation then uses SQL query to handle conflicts
    """

    try:
        buffer = StringIO()
        df.to_csv(buffer, header=False, index=False)
        buffer.seek(0)

        cur.copy_from(buffer, table_name, sep=',')

    except psycopg2.errors.UniqueViolation as e:
        cur.execute("ROLLBACK")
        for index, row in df.iterrows():
            cur.execute(table_insert, row.values)


def process_song_file(cur, filepath):
    """
    - Here we are going load the song file into a dataframe
    - Then we insert it to the relevant table if it contains just 1 entry
    """

    # open song file
    df = pd.read_json(filepath, typ='series')
    df = df.to_frame().transpose()

    if check_data_quality(df):
        # insert song record
        song_data = df.iloc[0][['song_id', 'title', 'artist_id', 'year', 'duration']].values
        cur.execute(song_table_insert, song_data)

        # insert artist record
        artist_data = df.iloc[0][['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
        cur.execute(artist_table_insert, artist_data)
    else:
        for index, row in df.iterrows():
            # insert song record
            song_data = df.iloc[index][['song_id', 'title', 'artist_id', 'year', 'duration']].values
            cur.execute(song_table_insert, song_data)

            # insert artist record
            artist_data = df.iloc[index][['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
            cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Here we are going save the dataframe in memory
    and use copy_from() to copy it to the table.
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df.ts, unit='ms')

    # transform time data records
    df_time = df.ts.dt
    time_data = zip(df.ts.values, df_time.hour, df_time.day, df_time.isocalendar().week, df_time.month, df_time.year, df_time.dayofweek)
    column_labels = ('timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(time_data, columns=column_labels)

    # if there's a non-unique primary key then using the SQL insert method to handle conflicts
    # if all keys are unique then using copy_from for speed
    optimize_insert(cur, time_df, 'time', time_table_insert)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    optimize_insert(cur, user_df, 'users', user_table_insert)

    songplay_data = []

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
        songplay_data.append([row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent.replace("\"", "")])

    # need to get songplay data to DF
    songplay_df = pd.DataFrame(songplay_data)

    # use a tab separator instead of comma because of commas within location and userAgent fields
    buffer = StringIO()
    songplay_df.to_csv(buffer, sep='\t', header=False, index=False)
    buffer.seek(0)

    cur.copy_from(buffer, 'songplays', sep='\t', columns=('start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent'))


def process_data(cur, conn, filepath, func):
    """
    - Crawl directory adding all filepaths to a list for further processing
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files, processing and inserting their data to the database
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f'{i}/{num_files} files processed.')


def main():
    """
    - Connect to database
    - Call data processing functions
    """

    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=transom")
    except psycopg2.Error as e:
        print("Error: Could not connect to the Database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not create a cursor")
        print(e)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
