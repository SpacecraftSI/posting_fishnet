# posting_fishnet
Create a fishnet (vector-raster hybrid) from vector data in a postgis database. Focusing on lines but could be repurposed. Maybe do a geom-type look first.. 

schematic for auth_class.py
class login:
    # Postgresql/Postgis database login details
    db = 'database'
    host = 'host'
    port = '1234'
    schem = 'public'

    user = 'user'
    pw = 'password'

    inputDb = 'inputtable'
    outputDb = 'outputtable'
    gridDb = 'polygon to insersect by'
    tempDb = 'temp'

