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

    start_time = datetime.now()

    datelist = []
    datelist = datefinder(conn)
    print(datelist)

    # not going to loop through dates yet while i'm testing
    temper(conn) # might not even need this and do away with the temp table?? --> might be useful to keep for testing, though.

    the_intersector(conn)

    len_sog(conn)

    # select duplicate row id's and make a sub-id with decimal
    data_caboose(conn)

    # STILL NEED TO DUMP TEMP INTO FINAL TABLE
    data_finalizer(conn)

    # probably don't need this but this closes out the cursor (and saves changes)- probably just good practice
    conn.commit()
    conn.close()

    now = datetime.now()
    duration = (now - start_time)

    print(duration)

def the_intersector(conn):
    # moves the selected data from the main database into the temp database
    cursor = conn.cursor()

    sql = (
    'INSERT INTO ' + auth_class.login.tempDb + '(segmentid, uid, mmsi, starttime, duration, isclassa, classais, classgen, name, isunique, lastchange, lenm, sogkt, inter, id1km, id2km, id4km, id8km) ' +
    'SELECT l.segmentid, l.uid, l.mmsi, l.starttime, l.duration, l.isclassa, l.classais, l.classgen, l.name, l.isunique, l.lastchange, l.lenm, l.sogkt, ' +
    'ST_INTERSECTION(l.geom, c.geom) AS inter, c.id_1km AS id1km, c.id_2km as id2km, c.id_4km as id4km, c.id_8km as id8km ' +
    'FROM ' + auth_class.login.inputDb + ' AS l, ' + auth_class.login.gridDb + ' AS c WHERE ST_INTERSECTS(l.geom, c.geom)'
    )

    cursor.execute(sql)

def len_sog(conn):
    # recalculates the length of each segment (now that some are cut) and recalculates the duration accordingly based on sog and length
    cursor = conn.cursor()

    # recalculates the length--> a couple things to note. This calculates CARTESIAN by default AND the units based on the srid (currently 3005).
    # use: ST_Distance_Sphere to calculate spherical if desired
    sql = 'UPDATE ' + auth_class.login.tempDb + ' SET lenm = ST_LENGTH(inter)'
    cursor.execute(sql)

    # recalculates the duration of the segment based on the newly calculated length and the originally calculated speed over ground
    # the equation for this is: lenm [length in meters] / (sog[kts]*0.514) = duration [in seconds]
    sql = 'UPDATE ' + auth_class.login.tempDb + ' SET duration = (lenm / (sogkt*0.514))'
    cursor.execute(sql)


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
            '(newid VARCHAR(50),' +
            'segmentId BIGINT,' +
            'uid BIGINT NOT NULL,' +
            'mmsi INT NOT NULL,' +
            'startTime TIMESTAMP WITHOUT TIME ZONE NOT NULL,' +
            'duration INT NOT NULL,' +
            'isClassA BOOL NOT NULL,' +
            'classAIS SMALLINT NOT NULL,' +
            'classGen SMALLINT NOT NULL,' +
            'name VARCHAR(20),' +
            'isUnique BOOL NOT NULL,' +
            'lastChange TIMESTAMP WITHOUT TIME ZONE NOT NULL,' +
            'lenM FLOAT,' +
            'sogKt FLOAT,' +
            'inter GEOMETRY(LineString,3005),' +
            'id1km VARCHAR(50),' +
            'id2km VARCHAR(50),' +
            'id4km VARCHAR(50),' +
            'id8km VARCHAR(50))'
             )

    cursor.execute(sql)


def datefinder(conn):
    # select dates that occur within the segments being worked on. This keeps the temp smaller and compartmentalizes the work a little bit
    cursor = conn.cursor()
    # sql = "SELECT starttime FROM " + auth_class.login.inputDb + " WHERE segmentid IN ({})".format(str(segList)[1:-1])
    sql = "SELECT starttime FROM " + auth_class.login.inputDb
    cursor.execute(sql)
    result = cursor.fetchall()

    # loop to convert datetime to date in a list
    datelist = []
    x = 0
    for i in result:
        datelist.append(result[x][0].strftime("'%Y-%m-%d'"))
        x += 1

    # only keeps unique dates to prevent unnecessary loops
    datelist = set(datelist)

    return datelist


def data_caboose(conn):
    # this function adds the associated cwsid1km (smallest resolution) to the segment id which will result in a totally unique id. Downside is that it is varchar.. so might be worth redoing this later.
    cursor = conn.cursor()
    sql = "UPDATE " + auth_class.login.tempDb + " SET newid = (segmentid || id1km)"
    cursor.execute(sql)

def data_finalizer(conn):
    cursor = conn.cursor()
    # check if table exists and if not creates a new output table
    sql = ('CREATE TABLE IF NOT EXISTS ' + auth_class.login.outputDb +
           ' (newid VARCHAR(50),' +
           'segmentId BIGINT,' +
           'uid BIGINT NOT NULL,' +
           'mmsi INT NOT NULL,' +
           'startTime TIMESTAMP WITHOUT TIME ZONE NOT NULL,' +
           'duration INT NOT NULL,' +
           'isClassA BOOL NOT NULL,' +
           'classAIS SMALLINT NOT NULL,' +
           'classGen SMALLINT NOT NULL,' +
           'name VARCHAR(20),' +
           'isUnique BOOL NOT NULL,' +
           'lastChange TIMESTAMP WITHOUT TIME ZONE NOT NULL,' +
           'lenM FLOAT,' +
           'sogKt FLOAT,' +
           'inter GEOMETRY(LineString,3005),' +
           'id1km VARCHAR(50),' +
           'id2km VARCHAR(50),' +
           'id4km VARCHAR(50),' +
           'id8km VARCHAR(50),'
           'UNIQUE(newid))'
           )
    cursor.execute(sql)

    # inserts data from the temporary table into the new main table -- does an upsert that skips on conflict with NEWID
    sql = ("INSERT INTO " + auth_class.login.outputDb + " (newid,segmentid,uid,mmsi,starttime,duration,isclassa,classais,classgen,name,isunique,lastchange,lenm,sogkt,inter,id1km,id2km,id4km,id8km)" +
        " SELECT newid,segmentid,uid,mmsi,starttime,duration,isclassa,classais,classgen,name,isunique,lastchange,lenm,sogkt,inter,id1km,id2km,id4km,id8km FROM " + auth_class.login.tempDb +
        " ON CONFLICT (newid) DO NOTHING")
    cursor.execute(sql)

if __name__ == ('__main__'):
    main()

