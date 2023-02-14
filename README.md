# posting_fishnet
Create a fishnet (vector-raster hybrid) from vector data in a postgis database. Focusing on lines but could be repurposed. Maybe do a geom-type look first.. 




Create a file called auth_class.py and copy paste this in and fill in with your credentials:

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