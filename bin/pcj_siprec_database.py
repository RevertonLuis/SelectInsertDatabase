import os
import sys

dir_root = os.path.abspath(
    os.path.dirname(os.path.realpath(__file__)) + '/../') + '/'
sys.path.insert(0, dir_root)

# Script that will insert the date form model run
command = ("/opt/miniconda3/envs/model_env/bin/python "
           "/discolocal/pcj_database/bin/"
           "pcj_load_insert_siprec_database.py "
           "-client 5 "
           "-siprec_hours 7 "
           "-siprec_days 2 "
           "-hourly_file_template "
           "/simepar/product/siprec/simepar/r1/"
           "hourly/%Y/%m/%d/siprec_v2_%Y_%m_%d_%H.nc "
           "-daily_file_template "
           "/simepar/product/siprec/simepar/r1/"
           "daily/%Y/%m/%d/siprec_v2_%Y_%m_%d_daily.nc")


def main():

    """ Routine that run the script that insert the PCJ data """

    print("\nRunning: %s\n" % command)
    os.system(command)


if __name__ == '__main__':

    # This will work in the following way:
    # 1) For the last 5 hours and 2 days read siprec
    # 2) Compute siprec for pcj boundaries
    # 3) Insert into the database

    # Run the script that will insert the data for PCJ boundaries
    main()
