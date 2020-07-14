#!/usr/bin/env python

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
        artists, albums, tracks = scan(folder)

        artists_id = {}
        for artist in artists:
            if artist in artists_id:
                continue
            try:
                command = """SELECT ID FROM artists WHERE NAME = %s ORDER BY ID ASC LIMIT 1"""
                cur.execute(command, (artist,))
                artist_row = cur.fetchone()
                if artist_row is not None:
                    artists_id[artist.lower()] = artist_row[0]
                else:
                    command = """INSERT INTO artists(NAME) VALUES(%s) RETURNING ID;"""
                    cur.execute(command, (artist,))
                    # get the generated id back
                    artist_row = cur.fetchone()
                    artists_id[artist.lower()] = artist_row[0]
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
        for album in albums.values():
            if 'artist' not in album:
                continue
            try:
                artist_id = None
                if album['artist'].lower() in artists_id:
                    artist_id = artists_id[album['artist'].lower()]
                else:
                    command = """SELECT ID FROM artists WHERE NAME = %s ORDER BY ID ASC LIMIT 1"""
                    cur.execute(command, (album['artist'],))
                    artist_row = cur.fetchone()
                    if artist_row is not None:
                        artist_id = artist_row[0]

                if artist_id is None:
                    continue
                command = """INSERT INTO albums(ARTISTID, NAME, RELEASEDATE) VALUES(%s, %s, %s) RETURNING ID;"""
                cur.execute(command, (artist_id, album['name'], f'01-01-{album["releasedate"]}'))
                # get the generated id back
                id = cur.fetchone()[0]
                album['ID'] = id
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
        for track in tracks.values():
            if 'album' not in track:
                continue
            album_id = None
            try:
                if track['album'].lower() in albums:
                    album_id = albums[track['album'].lower()]['ID']
                else:
                    command = """SELECT ID FROM albums WHERE NAME = %s ORDER BY ID ASC LIMIT 1"""
                    cur.execute(command, (track['album'],))
                    album_row = cur.fetchone()
                    if album_row is not None:
                        album_id = album_row[0]

                if album_id is None:
                    continue
                command = """INSERT INTO tracks(ALBUMID, NAME) VALUES(%s, %s) RETURNING ID;"""
                # TODO: Handle case with albums dict having the album without ID
                cur.execute(command, (album_id, track['title']))
                # get the generated id back
                track_id = cur.fetchone()[0]
                track['ID'] = track_id
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)

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
    artists = set()
    albums = {}
    tracks = {}

    for root, directories, filenames in os.walk(rootdir):
        for filename in filenames:
            song = mutagen.File(os.path.join(root, filename))
            if song is None:
                continue
            artist = None
            album = None
            warn_text = None
            if 'albumartist' in song:
                artist = song['albumartist'][0]
            # TODO: Se ci sono più artist aggiungili nel titolo come 'feat.'
            elif 'artist' in song:
                artist = song['artist'][0]
            if artist is not None:
                artists.add(artist)
            else:
                warn_text = f'{filename} does not have artist'

            if 'album' in song:
                if song['album'][0].lower() in albums:
                    album = albums[song['album'][0].lower()]
                else:
                    album = {'name': song['album'][0]}
                    albums[song['album'][0].lower()] = album
                if artist is not None:
                    album['artist'] = artist
                if 'date' in song:
                    album['releasedate'] = song['date'][0]
                else:
                    warn_text += ", date"
                    album['releasedate'] = '1900'
            else:
                warn_text += ", album"

            title = filename
            if 'title' in song:
                title = song['title'][0]
            track = {}
            if title.lower() in tracks:
                track = tracks[title.lower()]
            else:
                tracks[title.lower()] = track
            track['title'] = title
            if album is not None:
                track['album'] = album['name']

    return artists, albums, tracks


if __name__ == '__main__':
    connect()
