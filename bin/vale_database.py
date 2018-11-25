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

# Script that will insert the date form model run
command = ("/opt/miniconda3/envs/model_env/bin/python "
           "/discolocal/vale_thredds/bin/load_insert_vale_data.py "
           "-model_run %Y%m%d%H")


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


def check_last_inserted_data(st, date_ref):

    """ Routine that check tha last inserted data """

    # The vale points
    points = load_vale_points()

    # Dictionary with the models run inserted into database
    # for the models and points
    inserted_dates = []

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
                for r in response:
                    date = datetime.datetime(r[0].year,
                                             r[0].month,
                                             r[0].day,
                                             r[0].hour)
                    # if date >= date_ref then date
                    # can only be today + (00, 06, 12 or 18)
                    if date >= date_ref:
                        if date not in inserted_dates:
                            inserted_dates.append(date)

    cursor.close()
    db_connection.close()
    return inserted_dates


def insert_data(inserted_dates, date_ref):

    """ Routine that run the script that insert the Vale data """

    run_dates = [(date_ref + datetime.timedelta(hours=6 * h))
                 for h in range(4)]

    for date in inserted_dates:
        if date in run_dates:
            run_dates.remove(date)

    for date in run_dates:
        cmd = date.strftime(command)
        print("\nRunning: %s\n" % cmd)
        os.system(cmd)


if __name__ == '__main__':

    # Loading the project settings
    st = lib.load_settings.load_settings(
        dir_root + "metadata/settings.txt")

    # This will work in the following way:
    # 1) For each model (Vale1km and Vale5km)
    # 2) Retrieve the inserted model runs from the database
    # 3) For each run (00, 06, 12, 18) of the
    # atual day (datetime.datetime.now())
    # 4) If run is not in the runs from the database insert
    # The steps above will insert each model
    # only one time because of 2) and prevent the
    # script to be locked in a specific date because of 3)

    # Reference date
    date_ref = datetime.datetime.now().replace(hour=0, minute=0,
                                               second=0, microsecond=0)
    inserted_dates = check_last_inserted_data(st, date_ref)

    # Run the script that will insert the data for Vale for
    # the dates tha are not inserted yet
    insert_data(inserted_dates, date_ref)
