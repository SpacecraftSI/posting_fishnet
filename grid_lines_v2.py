# grid_lines_v2.py
# takes 'raw' ais line segments and breaks them into gridded cells and attributes each new or existing line with a grid id.
# to do this it also recalculates the speed and duration (based of proportion of line)
# this is stealing from filterer as that methodology seems to do better

# remember to double check the # filters section to see there's appropriate filters

# *** Note *** Because I forgot this when I came back to it..
# the basic 'gridjoin = False' config for this scripts keeps the lines 'as-is' just breaks them at the grid points.
# when 'gridjoin = True' then it actually aggregates them by grid cell BUT ALSO keeps the broken lines.
# might be worth adding a mode that doesn't keep the lines because that might just create unnecessary clutter in certain circumstances.

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

# gets rid of any features overlapping this polygon
landshape = "akbcwaor"

gridjoin = True
outgrid = ((auth_class.login.outputDb)+"gridagg")


def main():
    start_time = datetime.now()
    print("Creating: " + auth_class.login.outputDb)

    output_table()
    the_filter()
    the_intersector()
    length_calc()
    unique_id()

    conn.commit()

    if gridjoin == True:
        # this is for if we want to aggregate by grid square as an additional step to creating the 'gridded lines'
        # maybe move this to it's own program that can be called instead?
        print("Creating: " + outgrid)
        aisclass = grid_table()
        grid_join(aisclass)

    conn.commit()

    now = datetime.now()
    duration = (now - start_time)
    print("Completed in: " + str(duration))


def grid_join(aisclass):
    cursor = conn.cursor()
    for ais in aisclass:
        sql = ("UPDATE " + outgrid + " SET count" + str(ais) + " = newtab.counts, len" + str(ais) + " = newtab.len, elap" + str(ais) + " = newtab.elap, avg" + str(ais) + " = newtab.avg FROM " +
                "(SELECT ves.classgen, grid.id_1km, COUNT(DISTINCT ves.uid) AS counts, SUM(ves.lenm) AS len, SUM(ves.duration) AS elap, AVG(ves.sogkt) AS avg " +
                "FROM " + outgrid + " AS grid " +
                "JOIN " + auth_class.login.outputDb + " AS ves " +
                "ON grid.id_1km = ves.id1km " +
                "GROUP BY grid.id_1km, ves.classgen) AS newtab " +
                "WHERE newtab.classgen = " + str(ais) + " AND newtab.id_1km = "+outgrid+".id_1km")
        cursor.execute(sql)

    # aggregates for the total vessel class columns- note average is a seperate query as to not average the average
    sql = "UPDATE " + outgrid + " SET countagg = count0 + count1 + count2 + count3 + count4 + count5 + count6 + count7 + count8"
    cursor.execute(sql)
    sql = "UPDATE " + outgrid + " SET lenagg = len0 + len1 + len2 + len3 + len4 + len5 + len6 + len7 + len8"
    cursor.execute(sql)
    sql = "UPDATE " + outgrid + " SET elapagg =  elap0 + elap1 + elap2 + elap3 + elap4 + elap5 + elap6 + elap7 + elap8"
    cursor.execute(sql)

    # this is working but might be able to be simplified- it collects averages irrespective of class
    sql = ("UPDATE " + outgrid + " SET avgagg = newtab.avg FROM " +
            "(SELECT grid.id_1km, AVG(ves.sogkt) as avg " +
            "FROM " + outgrid + " AS grid " +
            "JOIN " + auth_class.login.outputDb + " AS ves " +
            "ON grid.id_1km = ves.id1km " +
            "GROUP BY grid.id_1km) AS newtab " +
            "WHERE newtab.id_1km = "+outgrid+".id_1km")
    cursor.execute(sql)


def grid_table():
    # fills out the grid table with required many columsn 4 x 9
    cursor = conn.cursor()
    sql = 'DROP TABLE IF EXISTS ' + outgrid
    cursor.execute(sql)

    sql = ("SELECT * INTO " + outgrid + " FROM " + auth_class.login.gridDb)
    cursor.execute(sql)

    aisclass = (0,1,2,3,4,5,6,7,8)
    columnnames = [['count', 'INT'], ['len','FLOAT'],['elap','FLOAT'], ['avg', 'FLOAT']]
    for columns in columnnames:
        # the if/else statement here ensures that averages don't default to zero, but rather null. As zero would be misleading if there are no vessels.
        if columns[0] == 'avg':
            sql = "ALTER TABLE " + outgrid + " ADD COLUMN " + columns[0] + "agg " + columns[1]
            cursor.execute(sql)
        else:
            # it's important that the default is set to null otherwise aggregation doesn't work properly with null values
            sql = "ALTER TABLE " + outgrid + " ADD COLUMN " + columns[0] + "agg " + columns[1] + " DEFAULT 0"
            cursor.execute(sql)

        for ais in aisclass:
            if columns[0] == 'avg':
                sql = "ALTER TABLE " + outgrid + " ADD COLUMN " + columns[0] + str(ais) + " " + columns[1]
                cursor.execute(sql)
            else:
                sql = "ALTER TABLE " + outgrid + " ADD COLUMN " + columns[0]+str(ais) + " " + columns[1] + " DEFAULT 0"
                cursor.execute(sql)
    return aisclass


def unique_id():
    # this function adds the associated cwsid1km (smallest resolution) to the segment id which will result in a totally unique id. Downside is that it is varchar.. so might be worth redoing this later.
    cursor = conn.cursor()
    sql = "UPDATE " + auth_class.login.outputDb + " SET newid = (segmentid || id1km)"
    cursor.execute(sql)


def length_calc():
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


def the_intersector():
    cursor = conn.cursor()
    sql = (
    'INSERT INTO ' + auth_class.login.outputDb + '(segmentid, uid, mmsi, starttime, duration, isclassa, classais, classgen, name, isunique, lastchange, lenm, sogkt, inter, id1km, id2km, id4km, id8km) ' +
    'SELECT l.segmentid, l.uid, l.mmsi, l.starttime, l.duration, l.isclassa, l.classais, l.classgen, l.name, l.isunique, l.lastchange, l.lenm, l.sogkt, ' +
    'ST_INTERSECTION(l.geom, c.geom) AS inter, c.id_1km AS id1km, c.id_2km as id2km, c.id_4km as id4km, c.id_8km as id8km ' +
    'FROM ' + auth_class.login.tempDb + ' AS l, ' + auth_class.login.gridDb + ' AS c WHERE ST_INTERSECTS(l.geom, c.geom) ON CONFLICT (newid) DO NOTHING')

    cursor.execute(sql)


def the_filter():
    cursor = conn.cursor()
    # drops existing temporary table
    sql = 'DROP TABLE IF EXISTS ' + auth_class.login.tempDb
    cursor.execute(sql)

    # moves filtered data into a temporary table
    sql = ("SELECT * INTO "+auth_class.login.tempDb+" FROM " + auth_class.login.inputDb + " WHERE sogkt < " + str(sog) + " AND lenm < " + str(length) + " AND duration < " + str(duration) + " AND duration > 0")
    cursor.execute(sql)

    # creates index --> it's a relatively light task and doing it every time just adds some bulletproofness
    sql = ('DROP INDEX IF EXISTS idx')
    cursor.execute(sql)
    sql = ("CREATE INDEX idx ON "+auth_class.login.tempDb+" USING gist (geom)")
    cursor.execute(sql)

    sql = "DROP TABLE IF EXISTS tempsland"
    cursor.execute(sql)

    sql = ("SELECT a.segmentid INTO tempsland from "+auth_class.login.tempDb+" as a, "+landtable+" as b where st_intersects(a.geom,b.geom)")
    cursor.execute(sql)
    sql = ("DELETE FROM "+auth_class.login.tempDb+" USING tempsland WHERE "+auth_class.login.tempDb+".segmentid = tempsland.segmentid")
    cursor.execute(sql)

    sql = "ALTER TABLE " +auth_class.login.tempDb+" ALTER COLUMN geom SET DATA TYPE geometry"
    cursor.execute(sql)
    sql = "ALTER TABLE " + auth_class.login.outputDb + " ALTER COLUMN inter SET DATA TYPE geometry"
    cursor.execute(sql)


def output_table():
    #drops output table if the name already exists
    cursor = conn.cursor()
    sql = "DROP TABLE IF EXISTS " + auth_class.login.outputDb
    cursor.execute(sql)
    # creates new output table
    sql = ('CREATE TABLE ' + auth_class.login.outputDb +
           ' (newid VARCHAR(50),' +
           'segmentId VARCHAR(50),' +
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
           'inter GEOMETRY(MultilineString,3005),' +
           'id1km VARCHAR(50),' +
           'id2km VARCHAR(50),' +
           'id4km VARCHAR(50),' +
           'id8km VARCHAR(50),'
           'UNIQUE(newid))'
           )
    cursor.execute(sql)


if __name__ == ('__main__'):
    main()