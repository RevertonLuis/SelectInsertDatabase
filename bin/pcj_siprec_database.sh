#!/bin/bash

#-----------------------------------------------------#
#                                                     #
#	Script that insert the data into database     #
#                                                     #
#                                                     #
#	Reverton                                      #
#	Data: 13/08/2018                              #
#                                                     #
#-----------------------------------------------------#

# Find the proper date
day="$(date +%d)"
month="$(date +%m)"
year="$(date +%Y)"
hour="$(date +%H)"


# Run the script that will insert the PCJ data in the database.
/opt/miniconda3/envs/model_env/bin/python /discolocal/pcj_database/bin/pcj_siprec_database.py 2> "/discolocal/pcj_database/logs/errors_siprec_$day$month$year$hour.txt" > "/discolocal/pcj_database/logs/logs_siprec_$day$month$year$hour.txt"
