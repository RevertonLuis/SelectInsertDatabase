import numpy
import multiprocessing
import math

def wrf_nearest_grid_point(wrflat, wrflon, estlat, estlon):

   #muda tudo para radianos
   wrflat = numpy.radians(wrflat[:,:])
   wrflon = numpy.radians(wrflon[:,:])
   estlat = numpy.radians(estlat)
   estlon = numpy.radians(estlon)

   #calcula distancia entre dois pontos em uma superficie esferica considerando um raio unitario
   d = numpy.cos(wrflat)*numpy.cos(estlat)*numpy.cos(wrflon-estlon)+numpy.sin(wrflat)*numpy.sin(estlat)
   d = numpy.arccos(d)
   minlat = numpy.where(d==d.min())[0].min()
   minlon = numpy.where(d==d.min())[1].min()

   return minlat, minlon
