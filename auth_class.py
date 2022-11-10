# auth_class.py
# class for server authentication and login

class login:
    # Postgresql/Postgis database login details
    db = 'database'
    host = 'localhost'
    port = '5432'
    schem = 'public'

    user = 'user'
    pw = 'password'

    inputDb = 'inputTable'
    outputDb = 'outputTable' #currently redundant as script uses inputDb for output

    tempDb = 'temp'

    # loading csv into test database
    loaderDb = 'loaderDb'
