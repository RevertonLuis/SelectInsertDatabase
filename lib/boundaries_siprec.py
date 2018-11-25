import psycopg2
import numpy

import lib.build_mask


class Boundaries():

    def __init__(self, client_code, siprec_files):

        # The client for each the boundaries will be loaded
        self.client_code = client_code

        # Siprec files
        self.siprec_files = siprec_files

        # Bounding box of all the boundaries
        # it will be used to cut the model lat/lon
        self.bounding_box_offset = 5
        self.bounding_box = {"lat_min": 999,
                             "lat_max": -999,
                             "lon_min": 999,
                             "lon_max": -999}

        # Boundaries dictionary
        self.boundaries = {}

    def client_boundaries_list(self):
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

    def siprec_bounding_box(self):

        """ Method cut the siprec data to the boundaries bounding box """

        slat = self.siprec_files["daily"][list(self.
                                          siprec_files["daily"]
                                          .keys())[0]]["siprec_lat"]
        slon = self.siprec_files["daily"][list(self.
                                          siprec_files["daily"]
                                          .keys())[0]]["siprec_lon"]

        slon, slat = numpy.meshgrid(slon, slat)
        latitudes_dim_size = slat.shape[0]
        longitudes_dim_size = slon.shape[1]

        # Cutting latlon
        latlon_c = numpy.where((self.bounding_box["lat_min"] <= slat) &
                               (self.bounding_box["lat_max"] >= slat) &
                               (self.bounding_box["lon_min"] <= slon) &
                               (self.bounding_box["lon_max"] >= slon))

        # Compute the model index that defines the bounding box
        if (latlon_c[0].min() - self.bounding_box_offset) >= 0:
            self.slat_min_index = (latlon_c[0].min() -
                                   self.bounding_box_offset)

        if (latlon_c[0].max() +
           self.bounding_box_offset <= latitudes_dim_size):
            self.slat_max_index = (latlon_c[0].max() +
                                   self.bounding_box_offset)

        if (latlon_c[1].min() - self.bounding_box_offset) >= 0:
            self.slon_min_index = (latlon_c[1].min() -
                                   self.bounding_box_offset)

        if (latlon_c[1].max() +
           self.bounding_box_offset <= longitudes_dim_size):
            self.slon_max_index = (latlon_c[1].max() +
                                   self.bounding_box_offset)

        # Cutting the latlon and prec for each date in self.siprec_files
        self.slat = slat[self.slat_min_index:self.slat_max_index,
                         self.slon_min_index:self.slon_max_index]

        self.slon = slon[self.slat_min_index:self.slat_max_index,
                         self.slon_min_index:self.slon_max_index]

        for tm in self.siprec_files.keys():
            for date in self.siprec_files[tm].keys():
                self.siprec_files[tm][date]["siprec_lat"] = self.slat
                self.siprec_files[tm][date]["siprec_lon"] = self.slon
                siprec_var = (self.siprec_files[tm][date]["siprec_var"]
                              [self.slat_min_index:self.slat_max_index,
                              self.slon_min_index:self.slon_max_index])
                self.siprec_files[tm][date]["siprec_var"] = siprec_var

    def build_masks(self):

        """ Method that will build the boundaries masks """

        for b in self.boundaries.keys():

            # Build the ensemble mask
            m = lib.build_mask.Mask(self.slat,
                                    self.slon,
                                    self.boundaries[b]["b_lat"],
                                    self.boundaries[b]["b_lon"])

            m.bounding_box()
            m.build_mask()

            # Update the self.boundaries with the mask
            self.boundaries[b].update({"b_mask": m.mask})

    def compute_boundaries_prec(self):

        """ Method that will compute the precipitation for each boundary  """

        for tm in self.siprec_files.keys():

            dates = self.siprec_files[tm].keys()

            for b in self.boundaries.keys():

                self.boundaries[b].update({tm: {}})
                
                for date in dates:

                    # Compute the precipitation for the boundary
                    prec = (numpy.
                        sum(self.siprec_files[tm][date]["siprec_var"] *
                            self.boundaries[b]["b_mask"]) /
                        numpy.
                        sum(self.boundaries[b]["b_mask"]))

                    # Add prec to a list of values for date
                    self.boundaries[b][tm][date] = prec
