import os
import sys
import datetime
import netCDF4
import numpy


class LoadModelData():

    """ Class that loads the model data """

    def __init__(self, model_url, forecast_hours, variables):
        super().__init__()

        self.model_url = model_url

        # Number of forecast hours (in case)
        # the user don't want load ALL the dates
        self.forecast_hours = forecast_hours + 1

        # In case a bounding box is provided
        self.bounding_box_offset = 5

        # Load the dimensions first
        self.load_dimensions()

        # Variables that will be loaded
        self.variables = variables

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
        if self.forecast_hours > (self.times_dim_size + 1):
            print("\nWarning: number of forecast hours (%i) > "
                  "number of model forecast dates (%i)\n" %
                  (self.forecast_hours - 1, self.times_dim_size))
            print("\nOnly %i forecast_hours will be loaded\n" %
                  (self.times_dim_size))
        else:
            self.times_dim_size = self.forecast_hours

        self.hight_dim_size = file.dimensions["bottom_top"].size
        self.latitudes_dim_size = file.dimensions["south_north"].size
        self.longitudes_dim_size = file.dimensions["west_east"].size
        self.run = datetime.datetime.strptime(file.START_DATE,
                                              "%Y-%m-%d_%H:%M:%S")
        file.close()

    def load_latlon(self, bounding_box=None):

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

        self.mlat_min_index = 0
        self.mlat_max_index = self.latitudes_dim_size
        self.mlon_min_index = 0
        self.mlon_max_index = self.longitudes_dim_size

        # If some bounding_box is provided then cut the
        # lat/lon from the models
        if bounding_box is not None:

            # Cutting latlon
            latlon_c = numpy.where((bounding_box["lat_min"] <= self.mlat) &
                                   (bounding_box["lat_max"] >= self.mlat) &
                                   (bounding_box["lon_min"] <= self.mlon) &
                                   (bounding_box["lon_max"] >= self.mlon))

            # Compute the model index that defines the bounding box
            if (latlon_c[0].min() - self.bounding_box_offset) >= 0:
                self.mlat_min_index = (latlon_c[0].min() -
                                       self.bounding_box_offset)

            if (latlon_c[0].max() +
               self.bounding_box_offset <= self.latitudes_dim_size):
                self.mlat_max_index = (latlon_c[0].max() +
                                       self.bounding_box_offset)

            if (latlon_c[1].min() - self.bounding_box_offset) >= 0:
                self.mlon_min_index = (latlon_c[1].min() -
                                       self.bounding_box_offset)

            if (latlon_c[1].max() +
               self.bounding_box_offset <= self.longitudes_dim_size):
                self.mlon_max_index = (latlon_c[1].max() +
                                       self.bounding_box_offset)

            # Cutting the latlon
            self.mlat = self.mlat[self.mlat_min_index:self.mlat_max_index,
                                  self.mlon_min_index:self.mlon_max_index]

            self.mlon = self.mlon[self.mlat_min_index:self.mlat_max_index,
                                  self.mlon_min_index:self.mlon_max_index]

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

    def compute_temperature(self, var_dict):

        """ Method that compute the temperature in Celcius """
        var = list(var_dict.keys())[0]
        var_dict.update({"Temperature_pos":
                         var_dict[var] - 273.15})

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

    def compute_pos_processes(self):

        """ Method that compute the pos processes of each variable """

        for var in self.variables.keys():
            method = getattr(self, "compute_" + var.lower())
            method(self.variables[var])

    def load_variables(self):

        """ Method that will load the variables from Vale models """

        for var in self.variables.keys():
            for var_name in self.variables[var].keys():

                # Building the url
                model_url_var = (self.model_url +
                                 "?%s[0:1:%i][%i:1:%i][%i:1:%i]" %
                                 (var_name,
                                  self.times_dim_size - 1,
                                  self.mlat_min_index,
                                  self.mlat_max_index - 1,
                                  self.mlon_min_index,
                                  self.mlon_max_index - 1))

                # Loading the variable
                file = netCDF4.Dataset(model_url_var)
                # the shape is (forecast_units, 1, 1)
                # so the model_url_var is properly cutting the unwanted
                # data
                variable = file.variables[var_name][:, :, :]
                file.close()

                # Update the variable dictionary
                self.variables[var][var_name] = variable
