import os
import sys

dir_root = os.path.abspath(
    os.path.dirname(os.path.realpath(__file__)) + '/../') + '/'
sys.path.insert(0, dir_root)

try:
    import lib.boundaries
    import lib.load_thredds_model
    import lib.insert_data
except ImportError:
    print("")
    print("Warning: sothing wrong with the project library, can't proceed")
    print("")
    sys.exit()

variables = {"Precipitation": {"RAINC": None,
                               "RAINNC": None},
             "Temperature": {"T2": None}}


def main(client, model_url, forecast_hours):

    # Load some preliminary model data
    model = lib.load_thredds_model.LoadModelData(model_url,
                                                 forecast_hours,
                                                 variables)

    # Load the boundaries
    boundaries = lib.boundaries.Boundaries(client, model)
    boundaries.client_boundaries_list()
    boundaries.load_boundaries_data()
    boundaries.load_model_data()
    boundaries.build_masks()
    boundaries.compute_boundaries_prec_1h_acc()
    boundaries.compute_boundaries_prec_24h_acc()
    boundaries.compute_boundaries_temp_average_1h() 
    boundaries.compute_boundaries_temp_min_max_24h()

    # Inserting the data into the database
    lib.insert_data.insert_data_into_database(boundaries.boundaries, model.run)


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

    # Number of forecast hours from the run
    if "-forecast_hours" in sys.argv:

        try:
            forecast_hours = int(sys.argv[sys.argv.
                                 index("-forecast_hours") + 1])
        except (ValueError, IndexError):
            print("")
            print("Number of forecast hours not provided")
            print("Example: python pcj_database.py "
                  "-client 5 "
                  "-forecast_hours 168")
            print("")
            exit()

    else:
        print("")
        print("Number of forecast hours not provided")
        print("Example: python pcj_database.py "
              "-client 5 "
              "-forecast_hours 168")
        print("")
        exit()

    # checking the args
    if "-model_url" in sys.argv:

        try:
            model_url = sys.argv[sys.argv.index("-model_url") + 1]
        except (ValueError, IndexError):
            print("")
            print("Model url not provided")
            print("Example: python pcj_database.py "
                  "-client 5 "
                  "-forecast_hours 168 "
                  "-model_url http://www.simepar.br/thredds"
                  "/dodsC/modelos/simepar/wrf/SSE/9km/raw/"
                  "2018/09/13/00/"
                  "SSE9km.op02_wrfout_2018-09-13_00.nc")
            print("")
            exit()

    else:
        print("")
        print("Model url not provided")
        print("Example: python pcj_database.py "
              "-client 5 "
              "-forecast_hours 168 "
              "-model_url http://www.simepar.br/thredds"
              "/dodsC/modelos/simepar/wrf/SSE/9km/raw/"
              "2018/09/13/00/"
              "SSE9km.op02_wrfout_2018-09-13_00.nc")
        print("")
        exit()

    main(client, model_url, forecast_hours)
