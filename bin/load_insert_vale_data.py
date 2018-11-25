import os
import sys
import datetime
import netCDF4
import psycopg2
import numpy

dir_root = os.path.abspath(
    os.path.dirname(os.path.realpath(__file__)) + '/../') + '/'
sys.path.insert(0, dir_root)

try:
    import lib.load_settings
    import lib.nearest_grid_point
    import lib.insert_data
except ImportError:
    print("")
    print("Warning: sothing wrong with the project library, can't proceed")
    print("")
    sys.exit()


# Direction Names
def compute_wind_direction_names_ids(direction):

    """ Routine that converts degrees to direction names """

    direction_names = ["" for x in range(direction.shape[0])]
    direction_ids = ["" for x in range(direction.shape[0])]

    for i in range(direction.shape[0]):
        if direction[i] >= 337.5 or direction[i] < 22.5:
            direction_names[i] = "North"
            direction_ids[i] = 0

        if direction[i] >= 22.5 and direction[i] < 67.5:
            direction_names[i] = "Northeast"
            direction_ids[i] = 1

        if direction[i] >= 67.5 and direction[i] < 112.5:
            direction_names[i] = "East"
            direction_ids[i] = 2

        if direction[i] >= 112.5 and direction[i] < 157.5:
            direction_names[i] = "Southeast"
            direction_ids[i] = 3

        if direction[i] >= 157.5 and direction[i] < 202.5:
            direction_names[i] = "South"
            direction_ids[i] = 4

        if direction[i] >= 202.5 and direction[i] < 247.5:
            direction_names[i] = "Southwest"
            direction_ids[i] = 5

        if direction[i] >= 247.5 and direction[i] < 292.5:
            direction_names[i] = "West"
            direction_ids[i] = 6

        if direction[i] >= 292.5 and direction[i] < 337.5:
            direction_names[i] = "Northwest"
            direction_ids[i] = 7

    return direction_names, direction_ids


class LoadValeData():

    """ Class that reads the Vale models data """

    def __init__(self, settings, model_url):
        super().__init__()

        self.st = settings
        self.model_url = model_url

        # Vale points dictionary
        self.points = {}

        # Load the Vale points first
        self.load_vale_points()

        # Load the dimensions first
        self.load_dimensions()

    def load_vale_points(self):

        """ Method that loads the Vale points from the database"""

        db_connection = psycopg2.connect("dbname='' "
                                         "user='' "
                                         "host='' "
                                         "password=''")

        # old
        #sql = ("select locid, locname, loclat, loclon "
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

        for p in response:
            self.points[int(p[0])] = {"description": p[1],
                                      "plat": float(p[2]),
                                      "plon": float(p[3])}

    def load_dimensions(self):

        """ Method that load the dimensions from the model """
        # There are 3 mandatory dimensions that must loaded
        # Loading lat/lon
        try:
            file = netCDF4.Dataset(self.model_url)
        except (OSError):
            print("\nSomething wrong with the model_url: %s\n" %
                  self.model_url)
            exit()

        self.times_dim_size = file.dimensions["Time"].size
        self.hight_dim_size = file.dimensions["bottom_top"].size
        self.latitudes_dim_size = file.dimensions["south_north"].size
        self.longitudes_dim_size = file.dimensions["west_east"].size
        self.run = datetime.datetime.strptime(file.START_DATE,
                                              "%Y-%m-%d_%H:%M:%S")
        file.close()

    def load_latlon(self):

        """ Method that load only lat/lon from the model url """

        model_url_latlon = (self.model_url +
                            "?XLAT[0:0][0:1:%i][0:1:%i],"
                            "XLONG[0:0][0:1:%i][0:1:%i]" %
                            (self.latitudes_dim_size - 1,
                             self.longitudes_dim_size - 1,
                             self.latitudes_dim_size - 1,
                             self.longitudes_dim_size - 1))

        # Loading lat/lon
        # REMARK: don't need try/except because load_dimensions already
        # checked if the model exists, if not the script is just terminated
        file = netCDF4.Dataset(model_url_latlon)

        # loading the whole lat/lon matrices
        # REMARK: the shape is (1, Nlat, Nlon), (1, Nlat, Nlon)
        # so the model_url_latlon is properly cutting the unwanted
        # data
        self.mlat = file.variables["XLAT"][0, :, :]
        self.mlon = file.variables["XLONG"][0, :, :]

        file.close()

    def load_times(self):

        """ Method that only load the Times from the model """

        model_url_times = (self.model_url + "?Times[0:1:%i]" %
                           (self.times_dim_size - 1))

        file = netCDF4.Dataset(model_url_times)
        self.times = file.variables["Times"][:]
        file.close()

        # Converting the times into datetime dates
        self.dates = []
        for d in self.times:
            date = datetime.datetime.strptime("".join(d.astype("str")),
                                              "%Y-%m-%d_%H:%M:%S")
            self.dates.append(date)

    def compute_nearest_points(self):

        """ Method that compute the nearest grid points """

        # Points outside from the model
        points_outside = []

        # For each vale point
        for p in self.points.keys():

            # Check if plat and plon are in the model region
            if (self.mlat.min() <= self.points[p]["plat"] and
               self.points[p]["plat"] <= self.mlat.max() and
               self.mlon.min() <= self.points[p]["plon"] and
               self.points[p]["plon"] <= self.mlon.max()):

                # Computing the model nearest lat/lon
                mlat_index, mlon_index = (lib.nearest_grid_point.
                                          wrf_nearest_grid_point(
                                              self.mlat,
                                              self.mlon,
                                              self.points[p]["plat"],
                                              self.points[p]["plon"]))

                # the model nearest points
                mlat = self.mlat[mlat_index, mlon_index]
                mlon = self.mlon[mlat_index, mlon_index]

                # Saving the lat_index and lon_index
                # For validation saving the nearest lat (mlat) and
                # lon (mlon) too
                self.points[p].update({"mlat_index": mlat_index,
                                       "mlon_index": mlon_index,
                                       "mlat": mlat,
                                       "mlon": mlon})
            else:
                print("\nWarning: point %s outside of model\n" %
                      (p)) # self.points[p]["description"]))
                points_outside.append(p)

        # Removing the points outside the model
        for p in points_outside:
            del self.points[p]

    def compute_precipitation(self, var_dict):

        """ Method that compute the precipitation from the loaded vars """

        # 1) Sum RAINC + RAINNC
        prec_pos = 0.
        for var in var_dict.keys():
            prec_pos = prec_pos + var_dict[var]

        # 2) Compute hourly acc
        prec_pos[1:] = prec_pos[1:] - prec_pos[:-1]

        # update the var_dict
        var_dict.update({"Precipitation_pos": prec_pos})

    def compute_temperature(self, var_dict):

        """ Method that compute the temperature in Celcius """

        var_dict.update({"Temperature_pos":
                         var_dict[self.st["Temperature"][1]] - 273.15})

    def compute_relativehumidity(self, var_dict):

        """ Method that compute the relative humidity from vars in var_dict """

        tmp = var_dict["T2"] - 273.15
        pres = var_dict["PSFC"] / 100.
        qv = var_dict["Q2"]

        # Calc saturation vapor pressure
        es = 6.112 * numpy.exp(17.67 * (tmp) / (tmp + 243.5))
        # Calc Relative Humidity
        rh = qv / (0.622 * es / (pres - (1. - 0.622) * es))
        # Limit values between 0 and 1. Model have some negative values for qv
        rh = numpy.where(rh > 1.0, 1.0, rh)
        rh = numpy.where(rh < 0.0, 0.0, rh)
        rh = 100. * rh

        # Update the dictionary
        var_dict.update({"RelativeHumidity_pos": rh})

    def compute_wind10m(self, var_dict):

        """ Method that compute the direction and magnitude of the wind10m """

        u10 = var_dict["U10"]
        v10 = var_dict["V10"]

        magnitude = numpy.sqrt(numpy.power(u10, 2) + numpy.power(v10, 2))
        direction = numpy.rad2deg(numpy.arctan2(-u10, -v10))
        direction = numpy.where(direction < 0, 360 + direction, direction)

        # Convert the degrees to direction names
        (direction_names,
         direction_ids) = compute_wind_direction_names_ids(direction)

        # Update the dictionary
        var_dict.update({"Wind10m_pos_magnitude": magnitude,
                         "Wind10m_pos_direction": direction,
                         "Wind10m_pos_direction_names": direction_names,
                         "Wind10m_pos_direction_ids": direction_ids})

    def compute_wind50m(self, var_dict):

        """ Method that compute the direction and magnitude of the wind50m """

        u50 = var_dict["U"]
        v50 = var_dict["V"]

        magnitude = numpy.sqrt(numpy.power(u50, 2) + numpy.power(v50, 2))
        direction = numpy.rad2deg(numpy.arctan2(-u50, -v50))
        direction = numpy.where(direction < 0, 360 + direction, direction)

        # Convert the degrees to direction names
        (direction_names,
         direction_ids) = compute_wind_direction_names_ids(direction)

        # Update the dictionary
        var_dict.update({"Wind50m_pos_magnitude": magnitude,
                         "Wind50m_pos_direction": direction,
                         "Wind50m_pos_direction_names": direction_names,
                         "Wind50m_pos_direction_ids": direction_ids})

    def compute_pos_processes(self):

        """ Method that compute the pos processes of each variable """

        for p in self.points.keys():
            for var in self.points[p]["variables"].keys():
                if self.st[var][0].lower() == "pos":
                    method = getattr(self, "compute_" + var.lower())
                    method(self.points[p]["variables"][var])

    def prepare_variables(self):

        """ Method that prepare the variables to be loaded """

        # Dictionary of the variables
        self.variables = {}

        for var in self.st["variables"]:
            self.variables[var] = {}
            for var_name in self.st[var][1:]:
                self.variables[var].update({var_name: None})

            # Check if the variable is raw or not
            # if raw ok if not then check if there is
            # a method to compute this variable
            # if not remove the var from the dictionary
            if self.st[var][0].lower() == "pos":
                if not hasattr(self, "compute_" + var.lower()):
                    del self.variables[var]
                    # print("\nWarning: variable %s not implemented\n" % var)

    def load_variables(self):

        """ Method that will load the variables from Vale models """

        for p in self.points.keys():

            # Putting the model run into the self.points
            self.points[p].update({"model_run": self.run})

            # Putting the dates in the self.points
            self.points[p].update({"dates": self.dates})

            # Clean the self.variables dictionary
            self.prepare_variables()

            mlat_index = self.points[p]["mlat_index"]
            mlon_index = self.points[p]["mlon_index"]

            for var in self.variables.keys():
                for var_name in self.variables[var].keys():

                    if var != "Wind50m":
                        # Building the url
                        model_url_var = (self.model_url +
                                         "?%s[0:1:%i][%i:1:%i][%i:1:%i]" %
                                         (var_name,
                                          self.times_dim_size - 1,
                                          mlat_index,
                                          mlat_index,
                                          mlon_index,
                                          mlon_index))
                        # Loading the variable
                        file = netCDF4.Dataset(model_url_var)
                        # the shape is (forecast_units, 1, 1)
                        # so the model_url_var is properly cutting the unwanted
                        # data
                        variable = file.variables[var_name][:, 0, 0]
                        file.close()
                    else:

                        # hight closest to 50m
                        mhight_index = int(self.st[var + "_eta_level"][0])

                        # Building the url
                        model_url_var = (self.model_url +
                                         "?%s[0:1:%i][%i:1:%i]"
                                         "[%i:1:%i][%i:1:%i]" %
                                         (var_name,
                                          self.times_dim_size - 1,
                                          mhight_index,
                                          mhight_index,
                                          mlat_index,
                                          mlat_index,
                                          mlon_index,
                                          mlon_index))

                        # Loading the variable
                        file = netCDF4.Dataset(model_url_var)
                        # the shape is (forecast_units, 1, 1)
                        # so the model_url_var is properly cutting the unwanted
                        # data
                        variable = file.variables[var_name][:, 0, 0, 0]
                        file.close()

                    # Update the variable dictionary
                    self.variables[var][var_name] = variable

            # Update the points dictionary with the variables
            self.points[p].update({"variables": self.variables})


if __name__ == "__main__":

    # Loading the project settings
    st = lib.load_settings.load_settings(
        dir_root + "metadata/settings.txt")

    # checking the args
    if "-model_run" in sys.argv:

        try:

            run = datetime.datetime.strptime(
                sys.argv[sys.argv.index("-model_run") + 1], "%Y%m%d%H")

        except (ValueError, IndexError):
            print("")
            print("Date not in the format YYYYMMDDHH")
            print("Example: python load_insert_vale_data.py "
                  "-model_run 2016020900 " +
                  "day 09, mmonth 10, year 2016 and hour 00")
            print("")
            sys.exit()

    else:

        print("")
        print("Date not in the format YYYYMMDDHH")
        print("Example: python load_insert_vale_data.py "
              "-model_run 2016020900 " +
              "day 09, mmonth 10, year 2016 and hour 00")
        print("")
        sys.exit()

    # Vale data
    vale_data = {}
    for model in st["models"]:
        model_url = run.strftime(st[model][0])
        model_vale = LoadValeData(st, model_url)
        model_vale.load_latlon()
        model_vale.load_times()
        model_vale.compute_nearest_points()
        model_vale.load_variables()
        model_vale.compute_pos_processes()

        vale_data[model] = model_vale

    # Inserting the data into the database
    lib.insert_data.insert_data_into_database(st, vale_data)
