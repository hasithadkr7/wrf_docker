import matplotlib.pyplot as plt
import numpy as np
from curw.rainfall.wrf.extraction import utils
from matplotlib import colors
from mpl_toolkits.basemap import Basemap
from curw.rainfall.wrf import utils as wrfutils

nc = '/home/nira/Desktop/wrfout_d03_2017-09-05_06:00:00'

lat_min = 5.722969
lon_min = 79.52146
lat_max = 10.06425
lon_max = 82.18992

clevs = 10 * np.array([0.1, 0.5, 1, 2, 3, 5, 10, 15, 20, 25, 30, 50, 75, 100])
basemap = Basemap(projection='merc', llcrnrlon=lon_min, llcrnrlat=lat_min, urcrnrlon=lon_max,
                  urcrnrlat=lat_max, resolution='h')
norm = colors.BoundaryNorm(boundaries=clevs, ncolors=256)

rf_vars = ['RAINC', 'RAINNC', 'SNOWNC', 'GRAUPELNC']

rf_values = utils.extract_variables(nc, rf_vars, lat_min, lat_max, lon_min, lon_max)

rf_values['PRECIP'] = rf_values[rf_vars[0]]
for i in range(1, len(rf_vars)):
    rf_values['PRECIP'] = rf_values['PRECIP'] + rf_values[rf_vars[i]]


def make_plots(v, out_dir, basemap, start=0, end=-1):
    start_rf = v['PRECIP'][0]
    wrfutils.create_dir_if_not_exists(out_dir)
    for j in range(start, end if end > 0 else len(v['PRECIP'])):
        out = out_dir + '/' + v['Times'][j] + 'cum.png'
        if j != 0:
            utils.create_contour_plot(v['PRECIP'][j] - start_rf, out, lat_min, lon_min, lat_max, lon_max, out,
                                      basemap=basemap, clevs=clevs, cmap=plt.get_cmap('jet'), overwrite=True, norm=norm)
        else:
            utils.create_contour_plot(v['PRECIP'][j], out, lat_min, lon_min, lat_max, lon_max, out,
                                      basemap=basemap, clevs=clevs, cmap=plt.get_cmap('jet'), overwrite=True, norm=norm)

make_plots(rf_values, '/home/nira/Desktop/temp', basemap)
