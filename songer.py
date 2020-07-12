#!/usr/bin/env python

import os
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
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


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

        artists, albums, tracks = scan()

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.commit()
            conn.close()
            print('Database connection closed.')


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
            if 'albumartist' in song:
                artist = song['albumartist'][0]
            # TODO: Se ci sono più artist aggiungili nel titolo come 'feat.'
            elif 'artist' in song:
                artist = song['artist'][0]
            if artist is not None:
                artists.add(artist)

            if 'album' in song:
                album = {'name': song['album']}
                if song['album'] in albums:
                    album = albums[song['album']]
                else:
                    albums[song['album']] = album
                if 'date' in song:
                    album['releasedate'] = song['date'][0]
                if artist is not None:
                    album['artist'] = artist

            if 'title' in song:
                track = {}
                if song['title'] in tracks:
                    track = tracks[song['title']]
                else:
                    tracks[song['title']] = track
                track['title'] = song['title']
                if album is not None:
                    track['album'] = album['name']

    return artists, albums, tracks


if __name__ == '__main__':
    scan()
