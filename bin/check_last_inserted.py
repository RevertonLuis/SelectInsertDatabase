import os
import sys
import psycopg2
import datetime

dir_root = os.path.abspath(
    os.path.dirname(os.path.realpath(__file__)) + '/../') + '/'
sys.path.insert(0, dir_root)

try:
    import lib.load_settings
except ImportError:
    print("")
    print("Warning: sothing wrong with the project library, can't proceed")
    print("")
    sys.exit()

# Global sql
sql_c = """select housourceid, houruntime, houdatetime,
           houlocationid, houvalue, houupdated
           from public.hourly
           WHERE
           housourceid=%i and
           houruntime=to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss') and
           houlocationid=%i"""


sql_run = """select houruntime
             from public.hourly
             WHERE
             housourceid=%i and
             houlocationid=%i"""

# Model source
source = {"Vale1km": 12,
          "Vale5km": 11}


def load_vale_points():

    """ Method that loads the Vale points from the database"""

    db_connection = psycopg2.connect("dbname='' "
                                     "user='' "
                                     "host='' "
                                     "password=''")

    # old
    # sql = ("select locid, locname, loclat, loclon "
    #       "from location "
    #       "where locmonitorid is not null")

    # New (there are new points in locmonitorid)
    sql = ("select locid, locname, loclat, loclon, locationgrouping "
           "from "
           "locationgrouping, "
           "location "
           "where "
           "lcggrouping=4 and "
           "locid=lcglocationid")

    cursor = db_connection.cursor()
    cursor.execute(sql)
    # db_connection.commit()
    response = cursor.fetchall()
    db_connection.close()

    points = []
    for p in response:
        points.append(int(p[0]))
    return points


def check_last_inserted_data():

    """ Routine that check tha last inserted data """

    # Loading the project settings
    st = lib.load_settings.load_settings(
        dir_root + "metadata/settings.txt")

    points = load_vale_points()

    db_connection = psycopg2.connect("dbname='' "
                                     "user='' "
                                     "host='' "
                                     "password=''")
    cursor = db_connection.cursor()

    for model in st["models"]:
        for p in points:

            sql = sql_run % (int(st[model + "_source"][0]),
                             p)
            cursor.execute(sql)

            response = cursor.fetchall()
            if len(response) > 0:
                last_inserted_run = max(response)
                print("Model: %s Vale Point: %i Last inserted run: %s" %
                      (model, p, last_inserted_run[0].strftime("%Y-%m-%d %H")))
    print()
    cursor.close()
    db_connection.close()


def check_the_data(model, point, model_run):

    """ Routine that checks the data itself """

    db_connection = psycopg2.connect("dbname='' "
                                     "user='' "
                                     "host='' "
                                     "password=''")
    cursor = db_connection.cursor()

    sql = sql_c % (source[model],
                   model_run.strftime("%Y-%m-%d %H:%M:%S"),
                   point)

    cursor.execute(sql)
    response = cursor.fetchall()
    if len(response) > 0:
        for r in response:

            sourceid = r[0]
            runtime = r[1]
            forecast_date = r[2]
            updated = r[5]
            data = r[4]

            print("Model: %s, Source: %i, Point: %i, Model Run: %s, "
                  "Forecast Date: %s, Updated: %s, Data:" %
                  (model, sourceid, point, runtime.strftime("%Y-%m-%d %H"),
                   forecast_date.strftime("%Y-%m-%d %H:%M"),
                   updated.strftime("%Y-%m-%d %H")))

            for k in data.keys():
                print(k, data[k])

        print()

    cursor.close()
    db_connection.close()


if __name__ == '__main__':
    check_last_inserted_data()

    # model = "Vale1km" or "Vale5km"
    # model = "Vale1km"
    # point = 9101, 9102, 9103 or 9104
    # point = 9101
    # runtime = datetime.datetime(2018, 8, 14, 6)
    # check_the_data(model, point, runtime)
