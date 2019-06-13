import curw.rainfall.wrf.extraction.utils as utils
import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.basemap import Basemap

lat_min = 5.722969
lon_min = 79.52146
lat_max = 10.06425
lon_max = 82.18992

# clevs = np.concatenate(([-1, 0], np.array([pow(2, i) for i in range(0, 9)])))
clevs = 10 * np.array([0.1, 0.5, 1, 2, 3, 5, 10, 15, 20, 25, 30])
basemap = Basemap(projection='merc', llcrnrlon=lon_min, llcrnrlat=lat_min, urcrnrlon=lon_max,
                  urcrnrlat=lat_max, resolution='h')


def make_plots(ii, v):
    start_rf = v['PRECIP'][23]
    for i in range(24, 73):
        out = 'wrf' + str(ii) + '/' + v['Times'][i] + 'cum.png'
        utils.create_contour_plot(v['PRECIP'][i] - start_rf, out, lat_min, lon_min, lat_max, lon_max, out,
                                  basemap=basemap, clevs=clevs, cmap=plt.get_cmap('gnuplot_r'), overwrite=True)


def make_asc(ii, v):
    start_rf = v['PRECIP'][23]
    lats = v['XLAT']
    lons = v['XLONG']
    cell_size = np.round(
        np.mean(np.append(lons[1:len(lons)] - lons[0: len(lons) - 1], lats[1:len(lats)] - lats[0: len(lats) - 1])),
        3)
    for i in range(24, 73):
        out = 'wrf' + str(ii) + '/' + v['Times'][i] + 'cum.asc'
        utils.create_asc_file(np.flip(v['PRECIP'][i] - start_rf, 0), lats, lons, out, cell_size=cell_size, overwrite=True)
