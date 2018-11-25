import os
import sys
import psycopg2
import datetime

dir_root = os.path.abspath(
    os.path.dirname(os.path.realpath(__file__)) + '/../') + '/'
sys.path.insert(0, dir_root)

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
           "/discolocal/pcj_database/bin/pcj_load_insert_model_database.py "
           "-client 5 "
           "-forecast_hours 180 "
           "-model_url "
           "http://www.simepar.br/thredds/dodsC/"
           "modelos/simepar/wrf/SSE/9km/raw/"
           "%Y/%m/%d/%H/SSE9km.op02_wrfout_%Y-%m-%d_%H.nc")


def load_pcj_boundaries():

    """ Load the boundaries for the client code """

    db_connection = psycopg2.connect("dbname='' "
                                     "user='' "
                                     "host='' "
                                     "password=''")
    sql = ("select locationid, locationname from customerlocation "
           "where customid=%i" % (5))

    cursor = db_connection.cursor()
    cursor.execute(sql)
    # db_connection.commit()
    response = cursor.fetchall()
    db_connection.close()

    boundaries = []
    for b in response:

        boundaries.append(int(b[0]))

    return boundaries


def check_last_inserted_data(date_ref):

    """ Routine that check tha last inserted data """

    # The pcj boundaries
    boundaries = load_pcj_boundaries()

    # Dictionary with the models run inserted into database
    # for the models and boundaries
    inserted_dates = []

    db_connection = psycopg2.connect("dbname='' "
                                     "user='' "
                                     "host='' "
                                     "password=''")
    cursor = db_connection.cursor()

    for b in boundaries:

        sql = sql_run % (9, b)
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

    # This will work in the following way:
    # 1) For hourly data
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
    inserted_dates = check_last_inserted_data(date_ref)

    # Run the script that will insert the data for Vale for
    # the dates tha are not inserted yet
    insert_data(inserted_dates, date_ref)
