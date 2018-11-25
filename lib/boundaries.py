import psycopg2
import numpy
import datetime

import lib.build_mask


class Boundaries():

    def __init__(self, client_code, model):

        # The client for each the boundaries will be loaded
        self.client_code = client_code

        # The forecast dates for each the statistics will be computed
        self.model = model

        # Bounding box of all the boundaries
        # it will be used to cut the model lat/lon
        self.bounding_box = {"lat_min": 999,
                             "lat_max": -999,
                             "lon_min": 999,
                             "lon_max": -999}

        # Boundaries dictionary
        self.boundaries = {}

    def client_boundaries_list(self, boundaries_ref=None):

        """ Load the boundaries for the client code """

        db_connection = psycopg2.connect("dbname='' "
                                         "user='' "
                                         "host='' "
                                         "password=''")
        sql = ("select locationid, locationname from customerlocation "
               "where customid=%i" % (self.client_code))

        cursor = db_connection.cursor()
        cursor.execute(sql)
        # db_connection.commit()
        response = cursor.fetchall()
        db_connection.close()

        self.b_dict = {}
        for b in response:

            if boundaries_ref is not None:
                # Only consider the boundaries that
                # are in self.table_boundaries
                if b[1] in boundaries_ref.keys():
                    self.b_dict[int(b[0])] = b[1]
            else:
                self.b_dict[int(b[0])] = b[1]

    def load_boundaries_data(self):

        """ Method that load the boundaries from the database """

        # Load boundary Lat-Lon
        db_connection = psycopg2.connect("dbname='' "
                                         "user='' "
                                         "host='' "
                                         "password=''")

        # for each boundary id in self.b_dict
        for b in self.b_dict.keys():

            sql = ("select st_astext(geom) as coord, "
                   "sbsname, sbsid "
                   "from subbasin where sbsid=%i" % (b))

            cursor = db_connection.cursor()
            cursor.execute(sql)
            # db_connection.commit()
            response = cursor.fetchall()

            # check if there is lats/lons for the boundary
            if response[0][0] is None:

                print("\nWarning: Latitudes and Longitudes for "
                      "the boundary %s are NOT in the database\n" % b)

            else:

                # updating the main dictionary
                self.boundaries[b] = {"b_name": response[0][1],
                                      "b_code": response[0][2]}

                # loadint the boundary lat/lon
                response = (response[0][0].
                            replace(')', '').
                            replace('(', '').
                            replace('MULTIPOLYGON', '').split(','))
                lat = []
                lon = []
                for i in range(len(response)):
                    line = response[i].split()
                    lon.append(float(line[0]))
                    lat.append(float(line[1]))

                self.boundaries[b].update({"b_lat": numpy.array(lat),
                                           "b_lon": numpy.array(lon)})

                # Update the bounding box
                if self.bounding_box["lat_min"] > min(lat):
                    self.bounding_box["lat_min"] = min(lat)
                if self.bounding_box["lon_min"] > min(lon):
                    self.bounding_box["lon_min"] = min(lon)
                if self.bounding_box["lat_max"] < max(lat):
                    self.bounding_box["lat_max"] = max(lat)
                if self.bounding_box["lon_max"] < max(lon):
                    self.bounding_box["lon_max"] = max(lon)

        # Closing the connection
        db_connection.close()

    def load_model_data(self):

        """ Method that will load the model data """

        self.model.load_times()
        self.model.load_latlon(self.bounding_box)
        self.model.load_variables()
        self.model.compute_pos_processes()

    def build_masks(self):

        """ Method that will build the boundaries masks """

        for b in self.boundaries.keys():

            # Build the ensemble mask
            m = lib.build_mask.Mask(self.model.mlat,
                                    self.model.mlon,
                                    self.boundaries[b]["b_lat"],
                                    self.boundaries[b]["b_lon"])

            m.bounding_box()
            m.build_mask()

            # Update the self.boundaries with the mask
            self.boundaries[b].update({"b_mask": m.mask})

    def compute_boundaries_prec_1h_acc(self):

        """ Method that will compute the hourly precipitation accumulation """

        dates = self.model.dates
        for b in self.boundaries.keys():

            self.boundaries[b].update({"prec_1h_acc": {}})

            for date, index in zip(dates, range(len(dates))):

                # Compute the precipitation for the boundary
                prec = (numpy.sum(abs(self.model.variables["Precipitation"]
                                                          ["Precipitation_pos"]
                                                          [index, :, :]) *
                                  self.boundaries[b]["b_mask"]) /
                        numpy.sum(self.boundaries[b]["b_mask"]))

                # Add prec to a list of values for date
                self.boundaries[b]["prec_1h_acc"][date] = prec

    def compute_boundaries_prec_24h_acc(self, forecast_days=7):

        """ Method that will compute the daily precipitation accumulation """

        # REMARK: the accumulations will be from Day 0 HL to (Day+1) 0 HL
        # For example: for the date 23-05-2018 the accumulation will be
        # from 23-05-2018 04:00:00 to 24-05-2018 03:00:00

        date_i = self.model.run
        date_i = date_i.replace(hour=0, minute=0, second=0, microsecond=0)
        dates_daily = [(date_i + datetime.timedelta(days=d))
                       for d in range(forecast_days)]

        dates = self.model.dates
        for b in self.boundaries.keys():

            self.boundaries[b].update({"prec_24h_acc": {}})

            for date in dates_daily:

                daily_acc = 0.
                for d, index in zip(dates, range(len(dates))):

                    if ((date + datetime.timedelta(hours=4)) <= d and
                       d <= (date + datetime.timedelta(days=1, hours=3))):

                        daily_acc = daily_acc + abs(self.model.variables
                                                    ["Precipitation"]
                                                    ["Precipitation_pos"]
                                                    [index, :, :])

                # Compute the precipitation for the boundary
                prec = (numpy.
                        sum(daily_acc *
                            self.boundaries[b]["b_mask"]) /
                        numpy.
                        sum(self.boundaries[b]["b_mask"]))

                # Add prec to a list of values for date
                self.boundaries[b]["prec_24h_acc"][date] = prec

    def compute_boundaries_temp_average_1h(self):

        """ Method that will compute the hourly tempmean """

        dates = self.model.dates
        for b in self.boundaries.keys():

            self.boundaries[b].update({"temp_1h_mean": {}})

            for date, index in zip(dates, range(len(dates))):

                # First find all values inside the boundary
                matrix_mask = numpy.where(self.boundaries[b]["b_mask"])
                boundary_matrix = (self.model.variables["Temperature"]
                                                       ["Temperature_pos"]
                                                       [index, :, :])
                boundary_matrix = boundary_matrix[matrix_mask]

                # Compute the tempmean for the boundary
                tempmean = boundary_matrix.mean()

                # Add tempmax and temmpin to a list of values for date
                self.boundaries[b]["temp_1h_mean"][date] = tempmean

    def compute_boundaries_temp_min_max_24h(self, forecast_days=7):

        """ Method that will compute daily tempmax and tempmin """

        # REMARK: the interval will be from Day 0 HL to (Day+1) 0 HL
        # For example: for the date 23-05-2018 the interval will be
        # from 23-05-2018 04:00:00 to 24-05-2018 03:00:00

        date_i = self.model.run
        date_i = date_i.replace(hour=0, minute=0, second=0, microsecond=0)
        dates_daily = [(date_i + datetime.timedelta(days=d))
                       for d in range(forecast_days)]

        dates = self.model.dates
        for b in self.boundaries.keys():

            self.boundaries[b].update({"temp_24h_min": {}})
            self.boundaries[b].update({"temp_24h_max": {}})

            for date in dates_daily:

                daily_temps = []

                for d, index in zip(dates, range(len(dates))):

                    if ((date + datetime.timedelta(hours=4)) <= d and
                       d <= (date + datetime.timedelta(days=1, hours=3))):

                        daily_temps.append(self.boundaries[b]
                                           ["temp_1h_mean"][d])

                # Add tempmax and tempmin to a list of values for date
                self.boundaries[b]["temp_24h_min"][date] = min(daily_temps)
                self.boundaries[b]["temp_24h_max"][date] = max(daily_temps)
