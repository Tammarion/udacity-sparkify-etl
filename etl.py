import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def get_connection():
    """Connect to the sparkifydb database using environment variables."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        dbname=os.getenv("DB_NAME", "sparkifydb"),
        user=os.getenv("DB_USER", "student"),
        password=os.getenv("DB_PASSWORD", "student")
    )


def process_song_file(cur, filepath):
    """Open a song JSON file and insert one record into songs and artists tables."""
    df = pd.read_json(filepath, lines=True)

    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)

    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Open a log JSON file and insert records into time, users, and songplays tables."""
    df = pd.read_json(filepath, lines=True)

    df = df[df['page'] == 'NextSong']
    df = df[df['userId'].astype(str).str.strip() != '']
    df['userId'] = df['userId'].astype(int)

    # time table
    t = pd.to_datetime(df['ts'], unit='ms')
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # users table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates(subset=['userId'])
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # songplays table
    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        songid, artistid = results if results else (None, None)

        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Walk a directory, find all JSON files, and apply a processing function to each."""
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Connect to sparkifydb, run the ETL pipeline, and close the connection."""
    conn = get_connection()
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data',  func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
