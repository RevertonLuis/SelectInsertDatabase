import numpy

import lib.polygons_mask as polygons_mask


def nearest_point(wrflat, wrflon, estlat, estlon):

    wrflat = numpy.radians(wrflat[:,:])
    wrflon = numpy.radians(wrflon[:,:])
    estlat = numpy.radians(estlat)
    estlon = numpy.radians(estlon)

    d = numpy.cos(wrflat)*numpy.cos(estlat)*numpy.cos(wrflon-estlon)+numpy.sin(wrflat)*numpy.sin(estlat)
    d = numpy.arccos(d)
    minlat = numpy.where(d==d.min())[0].min()
    minlon = numpy.where(d==d.min())[1].min()

    return minlat, minlon


class Mask():

    def init_masks(self):

        if (isinstance(self.lat, numpy.ndarray) and
            isinstance(self.lon, numpy.ndarray)):

            if self.lat.shape == self.lon.shape:

                # 2D lat-lon = 2D
                if len(self.lat.shape) == 2:
                    self.mask = numpy.zeros(self.lat.shape, 'f')
               
      
                # 1D lat-lon = 1D
                if len(self.lat.shape) == 1:
                    self.mask = numpy.zeros((self.lat.shape[0],
                                             self.lon.shape[0]), 'f')
                    self.case = 1 
         
            else:
                print("Incompatible Latitude and Longitude dimensions")

        if isinstance(self.lat, list) and isinstance(self.lon, list):

            if len(self.lat) != len(self.lon):

                # Caso em que lat-lon nao representam pares
                if len(self.lat) != len(self.lon):
                    self.mask = numpy.zeros((len(self.lat),
                                             len(self.lon)), 'f')
                    self.case = 2

    def bounding_box(self):
      
        # Polygon Bounding box
        if self.case == 0:
            self.bbox = numpy.where((self.lat <= max(self.lat_b)) &
                                    (self.lat >= min(self.lat_b)) &
                                    (self.lon <= max(self.lon_b)) &
                                    (self.lon >= min(self.lon_b)))
      
    def build_mask(self):

        # Load the instance that decides if a point is inside of a polygon
        pol = polygons_mask.Polygon(self.lat_b, self.lon_b, self.boundaries_inside) 
      
        # Decide if is inside
        for p in range(self.bbox[0].shape[0]):
            j = self.bbox[0][p]
            i = self.bbox[1][p]
            pol.decide(self.lat[j,i], self.lon[j,i])
            self.mask[j,i] = pol.dentro

        # Check the mask
        self.check_and_fix_mask() 

    def check_and_fix_mask(self):
        
        if self.mask.max() == 0.:
       
            # No point inside the polygon
            # Find the nearest point of the polygon
            # Point ul = upper left 
            # ( max(self.lat_b), min(self.lon_b) ) 
            ulj, uli = nearest_point(self.lat,
                                     self.lon,
                                     max(self.lat_b),
                                     min(self.lon_b))

            # Ponto br = bottom right
            # ( min(self.lat_b), max(self.lon_b) ) 
            brj, bri = nearest_point(self.lat,
                                     self.lon,
                                     min(self.lat_b),
                                     max(self.lon_b))

            # Change the points around the polygon in 1
            for j in range(brj, ulj+1):
                for i in range(uli, bri+1):
                    self.mask[j, i] = 1.

    def __init__(self, lat, lon, lat_b, lon_b, boundaries_inside=1):

        self.lat = lat
        self.lon = lon
        self.lat_b = lat_b
        self.lon_b = lon_b
        self.case = 0 # 2D Lat-Lon
        self.boundaries_inside = boundaries_inside

        # Init the masks
        self.init_masks()

