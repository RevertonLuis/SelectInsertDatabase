import os
import sys
import datetime
import netCDF4

dir_root = os.path.abspath(
    os.path.dirname(os.path.realpath(__file__)) + '/../') + '/'
sys.path.insert(0, dir_root)

try:
    import lib.boundaries_siprec
    import lib.insert_siprec_data
except ImportError:
    print("")
    print("Warning: sothing wrong with the project library, can't proceed")
    print("")
    sys.exit()

siprec_var_name = "SIPREC"


def load_siprec_files(siprec_hours,
                      siprec_days,
                      hourly_file_template,
                      daily_file_template):

    """ Routine that loads the siprec files """

    date = datetime.datetime.now().replace(hour=0, minute=0,
                                           second=0, microsecond=0)
    daily_dates = [(date - datetime.timedelta(days=d))
                   for d in range(siprec_days)]
    date = datetime.datetime.now().replace(minute=0,
                                           second=0, microsecond=0)
    hourly_dates = [(date - datetime.timedelta(hours=h))
                    for h in range(5)]

    siprec_files = {"hourly": {}, "daily": {}}

    # Loading the files
    for tm in ["hourly", "daily"]:
        file_template = hourly_file_template
        dates = hourly_dates
        siprec_lat_name = "latitudes"
        siprec_lon_name = "longitudes"

        if tm == "daily":
            file_template = daily_file_template
            dates = daily_dates
            siprec_lat_name = "latitude"
            siprec_lon_name = "longitude"

        for date in dates:
            siprec_file = date.strftime(file_template)
            if os.path.exists(siprec_file):
                file = netCDF4.Dataset(siprec_file)
                siprec_var = file.variables[siprec_var_name][0, :, :]
                siprec_lat = file.variables[siprec_lat_name][:]
                siprec_lon = file.variables[siprec_lon_name][:]
                file.close()
                siprec_files[tm][date] = {"siprec_var": siprec_var,
                                          "siprec_lat": siprec_lat,
                                          "siprec_lon": siprec_lon}
            else:
                print("")
                print("Warning: SIPREC file %s not found" % siprec_file)
                print("")
    return siprec_files


def main(client, siprec_hours, siprec_days,
         hourly_file_template, daily_file_template):

    siprec_files = load_siprec_files(siprec_hours,
                                     siprec_days,
                                     hourly_file_template,
                                     daily_file_template)

    # Loading boundaries and computing the precipitation for the boundaries
    boundaries = lib.boundaries_siprec.Boundaries(client,
                                                  siprec_files)
    boundaries.client_boundaries_list()
    boundaries.load_boundaries_data()
    boundaries.siprec_bounding_box()
    boundaries.build_masks()
    boundaries.compute_boundaries_prec()

    # Inserting the data into the database
    lib.insert_siprec_data.insert_data_into_database(boundaries.boundaries)


# -----------------------INICIO DO PROGRAMA----------------------------------
if __name__ == '__main__':

    if "-client" in sys.argv:

        try:
            client = int(sys.argv[sys.argv.index("-client") + 1])
        except (ValueError, IndexError):
            print("")
            print("Client not provided")
            print("Possible clients are 1 (Copel), "
                  "2 (CTG), 3 (Itaipu), 5 (PCJ) and others")
            print("Example: python pcj_database.py "
                  "-client 5")
            print("")
            exit()

    else:
        print("")
        print("Client not provided")
        print("Possible clients are 1 (Copel), "
              "2 (CTG), 3 (Itaipu), 5 (PCJ) and others")
        print("Example: python pcj_database.py "
              "-client 5")
        print("")
        exit()

    # Number of siprec hours from now
    if "-siprec_hours" in sys.argv:

        try:
            siprec_hours = int(sys.argv[sys.argv.
                               index("-siprec_hours") + 1])
        except (ValueError, IndexError):
            print("")
            print("Number of siprec hours not provided")
            print("Example: python pcj_load_insert_siprec_database.py "
                  "-client 5 "
                  "-siprec_hours 5")
            print("")
            exit()

    else:
        print("")
        print("Number of siprec hours not provided")
        print("Example: python pcj_load_insert_database.py "
              "-client 5 "
              "-siprec_hours 5")
        print("")
        exit()

    # Number of forecast hours from the run
    if "-siprec_days" in sys.argv:

        try:
            siprec_days = int(sys.argv[sys.argv.
                              index("-siprec_days") + 1])
        except (ValueError, IndexError):
            print("")
            print("Number of siprec days not provided")
            print("Example: python pcj_load_insert_siprec_database.py "
                  "-client 5 "
                  "-siprec_hours 5 "
                  "-siprec_days 2")
            print("")
            exit()

    else:
        print("")
        print("Number of siprec days not provided")
        print("Example: python pcj_load_insert_siprec_database.py "
              "-client 5 "
              "-siprec_hours 5 "
              "-siprec_days 2")
        print("")
        exit()

    if "-hourly_file_template" in sys.argv:

        try:
            hourly_file_template = sys.argv[sys.argv.
                                            index("-hourly_file_template") + 1]
        except (ValueError, IndexError):
            print("")
            print("SIPREC hourly file template no provided")
            print("Example: python pcj_load_insert_siprec_database.py "
                  "-client 5 "
                  "-siprec_hours 5 "
                  "-siprec_days 2 "
                  "-hourly_file_template /simepar/product/"
                  "siprec/simepar/r1/hourly/%Y/%m/%d/"
                  "siprec_v2_%Y_%m_%d_%H.nc")
            print("")
            exit()

    else:
        print("")
        print("SIPREC hourly file template no provided")
        print("Example: python pcj_load_insert_siprec_database.py "
              "-client 5 "
              "-siprec_hours 5 "
              "-siprec_days 2 "
              "-hourly_file_template /simepar/product/"
              "siprec/simepar/r1/hourly/%Y/%m/%d/"
              "siprec_v2_%Y_%m_%d_%H.nc")
        print("")
        exit()

    if "-daily_file_template" in sys.argv:

        try:
            daily_file_template = sys.argv[sys.argv.
                                           index("-daily_file_template") + 1]
        except (ValueError, IndexError):
            print("")
            print("SIPREC daily file template no provided")
            print("Example: python pcj_load_insert_siprec_database.py "
                  "-client 5 "
                  "-siprec_hours 5 "
                  "-siprec_days 2 "
                  "-hourly_file_template /simepar/product/"
                  "siprec/simepar/r1/hourly/%Y/%m/%d/"
                  "siprec_v2_%Y_%m_%d_%H.nc "
                  "-daily_file_template /simepar/product/"
                  "siprec/simepar/r1/daily/%Y/%m/%d/"
                  "siprec_v2_%Y_%m_%d_daily.nc")
            print("")
            exit()

    else:
        print("")
        print("SIPREC daily file template no provided")
        print("Example: python pcj_load_insert_siprec_database.py "
              "-client 5 "
              "-siprec_hours 5 "
              "-siprec_days 2 "
              "-hourly_file_template /simepar/product/"
              "siprec/simepar/r1/hourly/%Y/%m/%d/"
              "siprec_v2_%Y_%m_%d_%H.nc "
              "-daily_file_template /simepar/product/"
              "siprec/simepar/r1/daily/%Y/%m/%d/"
              "siprec_v2_%Y_%m_%d_daily.nc")
        print("")
        exit()

    main(client, siprec_hours, siprec_days,
         hourly_file_template, daily_file_template)
