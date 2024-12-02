# grid_aggregate.py

# In this case, specific to AIS Vessel Traffic Data

# 2024-11-20

# A version of grid_lines_v2.py that aggregates the grid by a cell attribute.
# Intended to be used to store temporal data in cells -- at least in theory -- we'll see if this works
# The plan is to use the already processed output of grid_lines_v2.py...

# makes sense to divide via vessel class, I think.

# just some thoughts here..
# I guess on possibility here would be use the aggregated months in the grid_lines_v2 output, but then make this file a way to add / subtract etc and do the final output. Aggregated months, or differecne between months or a combination of both.

# for now lets do this monthly.. basically no 'years' just aggregate whatevers in each

# schema for output of grid_liens_v2.py as of 2024-11-25

# NOTE! Removed gridPoly because instead of doing this on 'raw' lines this will be done *after* grid_lines_v2.py

from datetime import datetime
import psycopg2 as pg
import auth_class

# we'll see if we need this or not..
from osgeo import gdal
import os

# establish database connection
conn = pg.connect(host=auth_class.login.host,
                  port=auth_class.login.port,
                  dbname=auth_class.login.db,
                  user=auth_class.login.user,
                  password=auth_class.login.pw,
                  options='-c search_path=dbo,' + str(auth_class.login.schem))  # sets schema to public


# insert the name of the grid or polygon aggregating sql database table
aggList = ['testcwsagg_202005', 'testcwsagg_202006']

### SETTINGS ###
# set vessel class you care about-- might roll all vessel classes into one and make the below variable a list to iterate through but we'll see.
vesClass = '1'

# name of the output table- because the inputs for this are pretty custom instead of doing a automatic table name I'm letting the user (me, probably) create a new table name everytime.
outTableName = 'aggTest'


def main():
    ###
    start_time = datetime.now()
    ###

    print("\nStarting grid_aggregate.py . . .")

    # create dictionary of yyyymm format string based on input file (that should, in theory, have yyyymm as the trailing string
    fileDateDict = {item: item[-6:] for item in aggList}

    # create output table based on input list
    out_table(fileDateDict)

    for theTable, theYear in fileDateDict.items():
        print("\naggregating cells for " + theTable)

        cursor = conn.cursor()

        ##################### just test to see what's in the table to make sure everything is working
        sql = "SELECT * FROM " + theTable +" LIMIT 6;"
        cursor.execute(sql)
        rows = cursor.fetchall()

        headers = [desc[0] for desc in cursor.description]

        # Print headers
        print("\t".join(headers))
        print("-" * 50)

        # Print the rows
        for row in rows:
            print("\t".join(str(value) for value in row))

            ### The actual function.. ###

        sql = f"""
            UPDATE {outTableName} AS out
            SET 
                cnt_{vesClass}_{theYear} = src.count{vesClass},
                len_{vesClass}_{theYear} = src.len{vesClass},
                elp_{vesClass}_{theYear} = src.elap{vesClass}
            FROM {theTable}
            AS src
            WHERE out.id1km = src.id_1km;
        """

        cursor.execute(sql)

            ####################

    ### Commits changes to postgres server
    conn.commit()
    cursor.close()
    ###

    ###
    now = datetime.now()
    duration = (now - start_time)
    print("\nCompleted in: " + str(duration))
    ###



def out_table(fileDateDict):
    print('creating output table')
    cursor = conn.cursor()
    sql = "DROP TABLE IF EXISTS " + outTableName
    cursor.execute(sql)

    # create table
    sql = ('CREATE TABLE ' + outTableName + ' ' +
           '(id VARCHAR(50),' +
           'geom GEOMETRY(MultiPolygon,3005),' +
           'id1km VARCHAR(50) PRIMARY KEY, '
           )

    additional_columns = []
    for yyyymm in fileDateDict.values():
        additional_columns.append(f"cnt_{vesClass}_{yyyymm} INT")
        additional_columns.append(f"len_{vesClass}_{yyyymm} INT")
        additional_columns.append(f"elp_{vesClass}_{yyyymm} INT")

    # Join the additional columns with commas and add to the SQL
    sql += ", ".join(additional_columns)

    # Close the table definition
    sql += ')'

    cursor.execute(sql)

    sql = f"""
        INSERT INTO {outTableName} (id, id1km, geom)
        SELECT id, id_1km, geom
        FROM {auth_class.login.gridDb} 
        WHERE id_1km IS NOT NULL;
    """

    cursor.execute(sql)

    conn.commit()


if __name__ == ('__main__'):
    main()


