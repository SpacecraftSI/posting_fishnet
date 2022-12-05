# grid_lines.py
# takes 'raw' ais line segments and breaks them into gridded cells and attributes each new or existing line with a grid id.
# to do this it also recalculates the speed and duration (based of proportion of line)

# steps
#

import logging
from datetime import datetime
import psycopg2 as pg

import auth_class

# establish database connection
conn = pg.connect(host=auth_class.login.host,
                  port=auth_class.login.port,
                  dbname=auth_class.login.db,
                  user=auth_class.login.user,
                  password=auth_class.login.pw,
                  options='-c search_path=dbo,' + str(auth_class.login.schem))  # sets schema to public



def main():
    global conn

    temper(conn)

    conn.commit()

    conn.close()

def temper(conn):
    # this functionality checks if a temp folder already exists and drops the existing one if it does -- regardless the script will create a blank temp folder
    exists = False
    try:
        cursor = conn.cursor()
        cursor.execute("select exists(select relname from pg_class where relname='" + auth_class.login.tempDb + "')")
        # turns exists to True if a temp table already exists in the database
        exists = cursor.fetchone()[0]
    finally:
        pass

    # deletes existing temp table if exists is TRUE
    if exists == True:
        cursor = conn.cursor()
        sql = 'DROP TABLE ' + auth_class.login.tempDb
        cursor.execute(sql)
    else:
        pass

    # create temp table in database
    cursor = conn.cursor()
    sql =   ('CREATE TABLE ' + auth_class.login.tempDb + ' ' +
            '(segmentId BIGINT PRIMARY KEY,' +
            'uid BIGINT NOT NULL,' +
            'mmsi INT NOT NULL,' +
            'startTime TIMESTAMP WITHOUT TIME ZONE NOT NULL,' +
            'duration INT NOT NULL,' +
            'startLat FLOAT NOT NULL,' +
            'startLon FLOAT NOT NULL,' +
            'endLat FLOAT NOT NULL,' +
            'endLon FLOAT NOT NULL,' +
            'isClassA BOOL NOT NULL,' +
            'classAIS SMALLINT NOT NULL,' +
            'classGen SMALLINT NOT NULL,' +
            'name VARCHAR(20),' +
            'isUnique BOOL NOT NULL,' +
            'lastChange TIMESTAMP WITHOUT TIME ZONE NOT NULL,' +
            'geom GEOMETRY (LineString, 4326),' +
            'lenM FLOAT,' +
            'sogKt FLOAT,' +
            'gridid INT,' +
            'newlen FLOAT,' +
            'propelap FLOAT)')

    cursor.execute(sql)


if __name__ == ('__main__'):
    main()

