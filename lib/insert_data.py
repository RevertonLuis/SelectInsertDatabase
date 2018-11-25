import psycopg2
import datetime
# import time

# Global sqls
sql_update = """UPDATE public.hourly
                SET
                houvalue='%s',
                houupdated=to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss')
                WHERE
                housourceid=%i and
                houruntime=to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss') and
                houdatetime=to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss') and
                houlocationid=%i"""

sql_insert = """INSERT INTO public.hourly
                (housourceid, houruntime, houdatetime,
                 houlocationid, houvalue, houupdated)
                 VALUES
                (%i,
                 to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss'),
                 to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss'),
                 %i,
                 '%s',
                 to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss'))"""

sql_select = """SELECT housourceid, houruntime, houdatetime,
                houlocationid, houvalue, houupdated
                FROM public.hourly
                WHERE
                housourceid=%i and
                houruntime=to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss') and
                houdatetime=to_timestamp('%s', 'YYYY-MM-DD hh24:mi:ss') and
                houlocationid=%i"""


def data_to_json(st, vale_data, vale_jsons):

    """ Routine that convert the data into json """

    # For each model
    for model in vale_data:
        vale_jsons[model] = {}

        # For each point
        for p in vale_data[model].points.keys():

            vale_jsons[model][p] = {"tz": "UTC",
                                    "lat": vale_data[model].points[p]["plat"],
                                    "lon": vale_data[model].points[p]["plon"],
                                    "model_run":
                                    vale_data[model].points[p]["model_run"],
                                    "data": {}}

            # data must another dictionary with the variables
            for date, index in zip(vale_data[model].points[p]["dates"],
                                   range(len(vale_data[model].points[p]
                                             ["dates"]))):
                data = {}
                # date_tuple = (date.year, date.month, date.day, date.hour,
                #               date.second, date.microsecond, 0, 0, 0)

                # data["timestamp"] = time.mktime(date_tuple)

                for variable in vale_data[model].points[p]["variables"].keys():

                    # Variable name in the database
                    if variable.lower().find("wind") == -1:
                        var_name = st[variable + "_db_name"][0]
                        data[var_name] = str((vale_data[model].points[p]
                                             ["variables"][variable]
                                             [variable + "_pos"][index]))
                    else:
                        var_name_mag = st[variable + "_db_name"][0]
                        var_name_dir = st[variable + "_db_name"][1]
                        var_name_id = st[variable + "_db_name"][2]

                        data[var_name_mag] = str((vale_data[model].points[p]
                                                 ["variables"][variable]
                                                 [variable + "_pos_magnitude"]
                                                 [index]))
                        data[var_name_dir] = str((vale_data[model].points[p]
                                                 ["variables"][variable]
                                                 [variable +
                                                 "_pos_direction_names"]
                                                  [index]))
                        data[var_name_id] = str((vale_data[model].points[p]
                                                 ["variables"][variable]
                                                 [variable +
                                                 "_pos_direction_ids"]
                                                 [index]))

                data_str = '{'
                for k, i in zip(data.keys(), range(len(data.keys()))):
                    data_str = data_str + "\"%s\": \"%s\"" % (k, data[k])
                    if i < (len(data.keys()) - 1):
                        data_str = data_str + ", "

                data_str = data_str + '}'

                # update the key data of vale_json[model][p]
                vale_jsons[model][p]["data"][date] = data_str


def insert_data_into_database(st, vale_data):

    """ Routine that insert data into database """

    # First prepare the json data that will be inserted
    vale_jsons = {}
    data_to_json(st, vale_data, vale_jsons)

    db_connection = psycopg2.connect("dbname='' "
                                     "user='' "
                                     "host='' "
                                     "password=''")
    cursor = db_connection.cursor()

    # Inserting the data
    for model in vale_jsons.keys():

        for p in vale_jsons[model].keys():
            model_run = vale_jsons[model][p]["model_run"]
            dates = vale_jsons[model][p]["data"].keys()
            for date in dates:

                # First check if there are data (plural of datum) into database
                # if yes, then update if not, then insert

                # Check if there are data
                sql = sql_select % (int(st[model + "_source"][0]),
                                    model_run.strftime('%Y-%m-%d %H:%M:%S'),
                                    date.strftime('%Y-%m-%d %H:%M:%S'),
                                    p)

                cursor.execute(sql)
                response = cursor.fetchall()

                if len(response) != 0:

                    # There are data so update
                    sql = sql_update % (vale_jsons[model][p]["data"][date],
                                        datetime.datetime.now().
                                        strftime('%Y-%m-%d %H:%M:%S'),
                                        int(st[model + "_source"][0]),
                                        model_run.
                                        strftime('%Y-%m-%d %H:%M:%S'),
                                        date.strftime('%Y-%m-%d %H:%M:%S'),
                                        p)
                else:

                    # No data into the database so insert
                    sql = sql_insert % (int(st[model + "_source"][0]),
                                        model_run.
                                        strftime('%Y-%m-%d %H:%M:%S'),
                                        date.
                                        strftime('%Y-%m-%d %H:%M:%S'),
                                        p,
                                        vale_jsons[model][p]["data"][date],
                                        datetime.datetime.now().
                                        strftime('%Y-%m-%d %H:%M:%S'))

                cursor.execute(sql)
    db_connection.commit()
    cursor.close()
