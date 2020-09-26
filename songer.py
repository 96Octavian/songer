#!/usr/bin/env python
# TODO: Serious error handling

import os
import sys

import mutagen
from configparser import ConfigParser
import psycopg2


def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    return db


def insert_artist(cur, track, artists_id):
    if 'album' not in track:
        return
    if 'artist' not in track['album']:
        return
    if track['album']['artist'].lower() in artists_id:
        return
    try:
        artist = track['album']['artist']
        command = """SELECT ID FROM artists WHERE NAME = %s ORDER BY ID ASC LIMIT 1"""
        cur.execute(command, (artist,))
        artist_row = cur.fetchone()
        if artist_row is not None:
            artists_id[artist.lower()] = artist_row[0]
        else:
            command = """INSERT INTO artists(NAME) VALUES(%s) RETURNING ID;"""
            cur.execute(command, (artist,))
            # get the generated id back
            artist_row = cur.fetchone()  # TODO: can an INSERT return None?
            artists_id[artist.lower()] = artist_row[0]
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def insert_album(cur, track, albums_id, artists_id):
    if 'album' not in track:
        return
    if track['album']['name'].lower() in albums_id:
        return
    if 'artist' not in track['album']:
        return
    artist = track['album']['artist']
    album = track['album']
    artist_id = None
    try:
        command = """SELECT ID FROM albums WHERE NAME = %s ORDER BY ID ASC LIMIT 1"""
        cur.execute(command, (album['name'],))
        album_row = cur.fetchone()
        if album_row is not None:
            albums_id[album['name'].lower()] = album_row[0]
            return

        if artist.lower() in artists_id:
            artist_id = artists_id[artist.lower()]
        else:
            command = """SELECT ID FROM artists WHERE NAME = %s ORDER BY ID ASC LIMIT 1"""
            cur.execute(command, (artist,))
            artist_row = cur.fetchone()
            if artist_row is not None:
                artist_id = artist_row[0]

        if artist_id is not None:
            command = """INSERT INTO albums(ARTISTID, NAME, RELEASEDATE) VALUES(%s, %s, %s) RETURNING ID;"""
            cur.execute(command, (artist_id, album['name'], f'01-01-{album["releasedate"]}'))
            # get the generated id back
            album_id = cur.fetchone()[0]
            albums_id[album['name'].lower()] = album_id

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def insert_track(cur, track, tracks_id, albums_id):
    if track['title'].lower() in tracks_id:
        return
    if 'album' not in track:
        return
    album_id = None
    try:
        command = """SELECT ID FROM tracks WHERE NAME = %s ORDER BY ID ASC LIMIT 1"""
        cur.execute(command, (track['title'],))
        track_row = cur.fetchone()
        if track_row is not None:
            tracks_id[track['title'].lower()] = track_row[0]
            return

        if track['album']['name'].lower() in albums_id:
            album_id = albums_id[track['album']['name'].lower()]
        else:
            command = """SELECT ID FROM albums WHERE NAME = %s ORDER BY ID ASC LIMIT 1"""
            cur.execute(command, (track['album']['name'],))
            album_row = cur.fetchone()
            if album_row is not None:
                album_id = album_row[0]

        if album_id is not None:
            command = """INSERT INTO tracks(ALBUMID, NAME, TRACKNUMBER, FILEPATH) VALUES(%s, %s, %s, %s) RETURNING ID;"""
            cur.execute(command, (album_id, track['title'], track['tracknumber'], track['filepath']))
            # get the generated id back
            track_id = cur.fetchone()[0]
            tracks_id[track['title'].lower()] = track_id
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


# TODO: Refactor and separate functions
def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('artists',))
        if not cur.fetchone()[0]:
            command = (
                """
                CREATE TABLE artists (
                    ID SERIAL,
                    NAME VARCHAR NOT NULL DEFAULT '',
                    PICTURE VARCHAR,
                    PRIMARY KEY (ID)
                );
                """
            )
            cur.execute(command)
        cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('albums',))
        if not cur.fetchone()[0]:
            command = (
                """
                CREATE TABLE albums (
                    ID SERIAL,
                    ARTISTID INT NOT NULL,
                    NAME VARCHAR DEFAULT '',
                    RELEASEDATE DATE NOT NULL,
                    PICTURE VARCHAR,
                    PRIMARY KEY (ID),
                    FOREIGN KEY (ARTISTID) REFERENCES artists (ID) ON UPDATE CASCADE ON DELETE CASCADE,
                    CONSTRAINT releasedate_must_be_1st_jan CHECK ( date_trunc('year', RELEASEDATE) = RELEASEDATE )
                );
                """
            )
            cur.execute(command)
        cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('tracks',))
        if not cur.fetchone()[0]:
            command = (
                """
                CREATE TABLE tracks (
                ID SERIAL,
                ALBUMID INT NOT NULL,
                NAME VARCHAR DEFAULT '',
                TRACKNUMBER INT DEFAULT 1,
                FILEPATH VARCHAR NOT NULL,
                PRIMARY KEY (ID),
                FOREIGN KEY (ALBUMID) REFERENCES albums (ID) ON UPDATE CASCADE ON DELETE CASCADE
                );
                """
            )
            cur.execute(command)

        # TODO: I don't like this, use argparse
        folder = "Music"
        if len(sys.argv) == 2:
            folder = sys.argv[1]

        artists_id = {}
        albums_id = {}
        tracks_id = {}
        for track in scan(folder):
            insert_artist(cur, track, artists_id)
            insert_album(cur, track, albums_id, artists_id)
            insert_track(cur, track, tracks_id, albums_id)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.commit()
            conn.close()
            print('Database connection closed.')


# TODO: Handle duplicates
def scan(rootdir='.'):
    for root, directories, filenames in os.walk(rootdir):
        for filename in filenames:
            try:
                song = mutagen.File(os.path.join(root, filename))
                if song is None:
                    continue
                artist = None
                album = None
                warn_text = None
                if 'albumartist' in song:
                    artist = song['albumartist'][0]
                # TODO: Multiple artists must be added in a new column in the DB, and interface will shoe them in the
                #  title as '(feat. )'
                elif 'artist' in song:
                    artist = song['artist'][0]
                if artist is None:
                    warn_text = f'{filename} does not have artist'

                if 'album' in song:
                    album = {'name': song['album'][0]}
                    if artist is not None:
                        album['artist'] = artist
                    if 'date' in song:
                        album['releasedate'] = song['date'][0]
                    else:
                        if warn_text is None:
                            warn_text = f'{filename} does not have date'
                        else:
                            warn_text += ", date"
                        # TODO: Re-design the DB to add multiple artists and decide if date can be NULL or default
                        album['releasedate'] = '1900'
                else:
                    if warn_text is None:
                        warn_text = f'{filename} does not have album'
                    else:
                        warn_text += ", album"

                title = filename
                if 'title' in song:
                    title = song['title'][0]
                tracknumber = 1
                if 'tracknumber' in song:
                    tracknumber = int(song['tracknumber'][0])
                track = {'title': title, 'tracknumber': tracknumber, 'filepath': os.path.abspath(os.path.join(root, filename))}

                if album is not None:
                    track['album'] = album

                if warn_text is not None:
                    print(warn_text)

                yield track

            except:
                pass


if __name__ == '__main__':
    connect()
