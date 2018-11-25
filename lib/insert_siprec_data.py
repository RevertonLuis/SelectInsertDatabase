import psycopg2
import datetime
# import time

# Global sqls
sql_update_1h = """UPDATE public.hourly
                   SET
                   houvalue='%s',
                   houupdated=to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss')
                   WHERE
                   housourceid=%i and
                   houruntime=to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss') and
                   houdatetime=to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss') and
                   houlocationid=%i"""

sql_insert_1h = """INSERT INTO public.hourly
                   (housourceid, houruntime, houdatetime,
                    houlocationid, houvalue, houupdated)
                   VALUES
                   (%i,
                    to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss'),
                    to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss'),
                    %i,
                    '%s',
                    to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss'))"""

sql_select_1h = """SELECT housourceid, houruntime, houdatetime,
                   houlocationid, houvalue, houupdated
                   FROM public.hourly
                   WHERE
                   housourceid=%i and
                   houruntime=to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss') and
                   houdatetime=to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss') and
                   houlocationid=%i"""

sql_update_24h = """UPDATE public.daily
                    SET
                    daivalue='%s',
                    daiupdated=to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss')
                    WHERE
                    daisourceid=%i and
                    dairuntime=to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss') and
                    daidate=to_timestamp('%s', 'YYYY-MM-DD') and
                    dailocationid=%i"""

sql_insert_24h = """INSERT INTO public.daily
                    (daisourceid, dairuntime, daidate,
                     dailocationid, daivalue, daiupdated)
                     VALUES
                    (%i,
                     to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss'),
                     to_timestamp('%s', 'YYYY-MM-DD'),
                     %i,
                     '%s',
                     to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss'))"""

sql_select_24h = """SELECT daisourceid, dairuntime, daidate,
                    dailocationid, daivalue, daiupdated
                    FROM public.daily
                    WHERE
                    daisourceid=%i and
                    dairuntime=to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss') and
                    daidate=to_timestamp('%s', 'YYYY-MM-DD') and
                    dailocationid=%i"""


# Precipitation var name in the database
prec_db_name = "precIntensity"

# The SIPREC id
siprec_id = 1

# Variables for each time interval
tm_variables = {"hourly": "precIntensity",
                "daily": "precIntensity"}


def data_to_json(boundaries, boundaries_jsons):

    """ Routine that convert the data into json """

    # For each time interval tm (1h or 24h)
    for tm in tm_variables.keys():

        boundaries_jsons[tm] = {}

        # For each point
        for b in boundaries.keys():
            boundaries_jsons[tm][b] = {"tz": "UTC",
                                       "lat": boundaries[b]["b_lat"],
                                       "lon": boundaries[b]["b_lon"],
                                       "data": {}}

            dates = list(boundaries[b][tm].keys())
            dates.sort()

            for date, index in zip(dates, range(len(dates))):

                # data must be another dictionary with the variables
                data = {}

                # date_tuple = (date.year, date.month, date.day, date.hour,
                #               date.second, date.microsecond, 0, 0, 0)

                # data["timestamp"] = time.mktime(date_tuple)

                # Variable name in the database
                var_name = tm_variables[tm]
                data[var_name] = str((boundaries[b][tm][date]))

                data_str = '{'
                for k, i in zip(data.keys(), range(len(data.keys()))):
                    data_str = data_str + "\"%s\": \"%s\"" % (k, data[k])
                    if i < (len(data.keys()) - 1):
                        data_str = data_str + ", "

                data_str = data_str + '}'

                # update the key data of vale_json[model][p]
                boundaries_jsons[tm][b]["data"][date] = data_str


def insert_data_into_database(boundaries):

    """ Routine that insert data into database """

    # First prepare the json data that will be inserted
    boundaries_jsons = {}
    data_to_json(boundaries, boundaries_jsons)

    db_connection = psycopg2.connect("dbname='' "
                                     "user='' "
                                     "host='' "
                                     "password=''")
    cursor = db_connection.cursor()

    # Inserting the data
    # For each time interval (hourly and daily)
    for tm in tm_variables.keys():

        if tm == "hourly":
            sql_select = sql_select_1h
            sql_insert = sql_insert_1h
            sql_update = sql_update_1h

        if tm == "daily":
            sql_select = sql_select_24h
            sql_insert = sql_insert_24h
            sql_update = sql_update_24h

        for b in boundaries_jsons[tm].keys():
            dates = boundaries_jsons[tm][b]["data"].keys()
            for date in dates:

                # First check if there are data (plural of datum) into database
                # if yes, then update if not, then insert

                # Check if there are data
                sql = sql_select % (siprec_id,
                                    date.strftime('%Y-%m-%d %H:%M:%S'),
                                    date.strftime('%Y-%m-%d %H:%M:%S'),
                                    b)

                cursor.execute(sql)
                response = cursor.fetchall()

                if len(response) != 0:

                    # There are data so update
                    sql = sql_update % (boundaries_jsons[tm][b]["data"][date],
                                        datetime.datetime.now().
                                        strftime('%Y-%m-%d %H:%M:%S'),
                                        siprec_id,
                                        date.
                                        strftime('%Y-%m-%d %H:%M:%S'),
                                        date.strftime('%Y-%m-%d %H:%M:%S'),
                                        b)
                else:

                    # No data into the database so insert
                    sql = sql_insert % (siprec_id,
                                        date.
                                        strftime('%Y-%m-%d %H:%M:%S'),
                                        date.
                                        strftime('%Y-%m-%d %H:%M:%S'),
                                        b,
                                        boundaries_jsons[tm][b]["data"][date],
                                        datetime.datetime.now().
                                        strftime('%Y-%m-%d %H:%M:%S'))

                cursor.execute(sql)

    db_connection.commit()
    cursor.close()
