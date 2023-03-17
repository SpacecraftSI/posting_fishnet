# grid_lines.py
# takes 'raw' ais line segments and breaks them into gridded cells and attributes each new or existing line with a grid id.
# to do this it also recalculates the speed and duration (based of proportion of line)


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

# this could be moved into auth class really or just do away with auth class all together is the other option
landtable = 'akbcwaor'

# filters
sog = 87
length = 10000
duration = 21600



def main():

    start_time = datetime.now()

    #drops output table if the name already exists
    cursor = conn.cursor()
    sql = "DROP TABLE IF EXISTS " + auth_class.login.outputDb
    cursor.execute(sql)

    # creates new output table
    sql = ('CREATE TABLE ' + auth_class.login.outputDb +
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

    # creates of list of all unique dates in the dataset for looping- hopefully deals with memory issues.
    print("TEST 1")
    sql = "SELECT starttime FROM " + auth_class.login.inputDb
    cursor.execute(sql)
    result = cursor.fetchall()
    dateList = []
    x = 0
    for i in result:
        dateList.append(result[x][0].strftime("'%Y-%m-%d'"))
        x += 1
    # only keeps unique dates to prevent unnecessary loops
    dateList = set(dateList)



    for date in dateList:
        sub_time = datetime.now()

        print("TEST 2")
        filterer(conn, date)

        #the_intersector(conn)

        #len_sog(conn)

        # select duplicate row id's and make a sub-id with decimal
        #id_caboose(conn)

        # probably don't need this but this closes out the cursor (and saves changes)- probably just good practice
        conn.commit()

        current_time = datetime.now()
        sub_duration = (current_time - sub_time)
        print(str(date) + " took " + str(sub_duration) + " to processes.")

    conn.close()

    # calcs total processing time
    now = datetime.now()
    duration = (now - start_time)
    print("Total processing time: " + str(duration))

def the_intersector(conn):
    # moves the selected data from the main database into the temp database
    cursor = conn.cursor()

    # makes sure an up-to-date index exists to keep things speedy
    sql = ('DROP INDEX IF EXISTS idx')
    cursor.execute(sql)
    sql = ('CREATE INDEX idx ON temp USING gist (geom)')
    cursor.execute(sql)

    sql = (
    'INSERT INTO ' + auth_class.login.outputDb + '(segmentid, uid, mmsi, starttime, duration, isclassa, classais, classgen, name, isunique, lastchange, lenm, sogkt, inter, id1km, id2km, id4km, id8km) ' +
    'SELECT l.segmentid, l.uid, l.mmsi, l.starttime, l.duration, l.isclassa, l.classais, l.classgen, l.name, l.isunique, l.lastchange, l.lenm, l.sogkt, ' +
    'ST_INTERSECTION(l.geom, c.geom) AS inter, c.id_1km AS id1km, c.id_2km as id2km, c.id_4km as id4km, c.id_8km as id8km ' +
    'FROM ' + auth_class.login.tempDb + ' AS l, ' + auth_class.login.gridDb + ' AS c WHERE ST_INTERSECTS(l.geom, c.geom) ON CONFLICT (newid) DO NOTHING'
    )

    cursor.execute(sql)


def len_sog(conn):
    # recalculates the length of each segment (now that some are cut) and recalculates the duration accordingly based on sog and length
    cursor = conn.cursor()

    # recalculates the length--> a couple things to note. This calculates CARTESIAN by default AND the units based on the srid (currently 3005).
    # use: ST_Distance_Sphere to calculate spherical if desired
    sql = 'UPDATE ' + auth_class.login.outputDb + ' SET lenm = ST_LENGTH(inter)'
    cursor.execute(sql)

    # recalculates the duration of the segment based on the newly calculated length and the originally calculated speed over ground
    # the equation for this is: lenm [length in meters] / (sog[kts]*0.514) = duration [in seconds]
    sql = 'UPDATE ' + auth_class.login.outputDb + ' SET duration = (lenm / (sogkt*0.514))'
    cursor.execute(sql)


def filterer(conn, date):
    cursor = conn.cursor()
    sql = "DROP TABLE IF EXISTS " + auth_class.login.tempDb
    cursor.execute(sql)

    # create temp table in database
    """
    sql = ('CREATE TABLE ' + auth_class.login.tempDb + ' ' +
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
    """

    # selects filtered data both geographically, and statistically, into the temp. Reduces the amount of data that needs to be processed for the intersection increasing performance.
    sql = ("SELECT * INTO "+auth_class.login.tempDb+" FROM " + auth_class.login.inputDb + " WHERE CAST(starttime AS DATE)  = " + date + " AND sogkt < "+str(sog)+" and lenm < "+str(length)+" and duration < "+str(duration))
    cursor.execute(sql)
    print("statistical filter compete")

    conn.commit()

    #sql = ("SELECT a.segmentid FROM temps AS a, akbcwaor AS b WHERE st_intersects(a.geom,b.geom)")
    #cursor.execute(sql)
    """
    landids = [r[0] for r in cursor.fetchall()]
    landids = tuple(landids)
    print(len(landids))
    print("land select complete")

    sql = ("DELETE FROM " + auth_class.login.tempDb + " WHERE segmentid IN %s")
    cursor.execute(sql, (landids,))
    print("land selected segments deleted from table")
    """


def id_caboose(conn):
    # this function adds the associated cwsid1km (smallest resolution) to the segment id which will result in a totally unique id. Downside is that it is varchar.. so might be worth redoing this later.
    cursor = conn.cursor()
    sql = "UPDATE " + auth_class.login.outputDb + " SET newid = (segmentid || id1km)"
    cursor.execute(sql)


if __name__ == ('__main__'):
    main()

