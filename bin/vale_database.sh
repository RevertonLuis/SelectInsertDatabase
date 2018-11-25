#!/bin/bash

#-----------------------------------------------------#
#                                                     #
#	Script that insert the data into database         #
#                                                     #
#                                                     #
#	Reverton                                          #
#	Data: 13/08/2018                                  #
#                                                     #
#-----------------------------------------------------#

# Find the proper date
day="$(date +%d)"
month="$(date +%m)"
year="$(date +%Y)"
hour="$(date +%H)"


# Run the script that will insert the Vale data in the database.
/opt/miniconda3/envs/model_env/bin/python /discolocal/vale_thredds/bin/vale_database.py 2> "/discolocal/vale_thredds/logs/errors_$day$month$year$hour.txt" > "/discolocal/vale_thredds/logs/logs_$day$month$year$hour.txt"
