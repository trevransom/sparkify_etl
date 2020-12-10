import os
from io import StringIO
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    # print(filepath)
    df = pd.read_json(filepath, typ='series')
    df = df.to_frame().transpose()
    # print(df.head(10))

    # perhaps put a test making sure that the df doesn't have more than 1 row 
    # if it doesn't then just use it if it does then separate or iterate over the rows

    # insert song record
    song_data = df.iloc[0][['song_id', 'title', 'artist_id', 'year', 'duration']].values
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df.iloc[0][['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Here we are going save the dataframe in memory
    and use copy_from() to copy it to the table
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    # t = pd.read_json(filepath)
    df['ts'] = pd.to_datetime(df.ts, unit='ms')

    # insert time data records
    df_time = df.ts.dt
    time_data = zip(df.ts.values, df_time.hour, df_time.day, df_time.isocalendar().week, df_time.month, df_time.year, df_time.dayofweek)

    time_data = zip(df.ts.values, df_time.hour, df_time.day, df_time.isocalendar().week, df_time.month, df_time.year, df_time.dayofweek)
    column_labels = ('timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(time_data, columns=column_labels)

    # save dataframe to an in memory buffer
    buffer = StringIO()
    time_df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)

    cur.copy_from(buffer, 'time', sep=',')

    # wherever there's a cur.execute I should swap it out for the copy_from function

    # for i, row in time_df.iterrows():
    #     cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # save dataframe to an in memory buffer
    buffer = StringIO()
    user_df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)

    cur.copy_from(buffer, 'users', sep=',')

    # insert user records
    # for i, row in user_df.iterrows():
    #     cur.execute(user_table_insert, row)

    # make a query (or list comprehension query) to get all ordered songids and artist ids
    # then merge that into a new df with a similar list comprehension for the other fields

    # x2 = [[row.ts, row.userId, row.level, row.sessionId, row.location, row.userAgent] for index, row in df.iterrows()]
    # print(x2)

    # okay cool so we got the list comprehension for the other rows, but now i'm wondering
    # we're gonna have to write something fairly complex to grab the other things and again loop through the df - would it make more sense at this point just to loop through the df once
    # okay  - but if we loop then we still ahve to save all the id data into a list of some sort which we'll then have to add to a custom df
    songplay_data = []

    # insert songplay records
    for index, row in df.iterrows():
        # print(row)
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        # print(row.userAgent)
        # insert songplay record
        songplay_data.append([row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent.replace("\"", "")])
        # for this since we're going row by row maybe I should store all the data up in a list and then batch copy it to the table instead of a row by row insert

    # need to get songplay data to DF
    songplay_df = pd.DataFrame(songplay_data)

    # use a tab separator instead of comma because of commas within location and userAgent fields
    buffer = StringIO()
    songplay_df.to_csv(buffer, sep='\t', header=False, index=False)
    buffer.seek(0)

    cur.copy_from(buffer, 'songplays', sep='\t', columns=('start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent'))


def process_data(cur, conn, filepath, func):
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
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=transom password=student")
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
