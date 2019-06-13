import json
import logging
import math
import os
import unittest
import datetime as dt

import imageio
import matplotlib
import numpy as np
from numpy.lib.recfunctions import append_fields
from curwmysqladapter import MySQLAdapter
from mpl_toolkits.basemap import Basemap
from netCDF4._netCDF4 import Dataset

from curw.rainfall.wrf import utils
from curw.rainfall.wrf.resources import manager as res_mgr

matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import colors


def extract_time_data(nc_f):
    nc_fid = Dataset(nc_f, 'r')
    times_len = len(nc_fid.dimensions['Time'])
    try:
        times = [''.join(x) for x in nc_fid.variables['Times'][0:times_len]]
    except TypeError:
        times = np.array([''.join([y.decode() for y in x]) for x in nc_fid.variables['Times'][:]])
    nc_fid.close()
    return times_len, times


def get_two_element_average(prcp, return_diff=True):
    avg_prcp = (prcp[1:] + prcp[:-1]) * 0.5
    if return_diff:
        return avg_prcp - np.insert(avg_prcp[:-1], 0, [0], axis=0)
    else:
        return avg_prcp


def extract_points_array_rf_series(nc_f, points_array, boundaries=None, rf_var_list=None, lat_var='XLAT',
                                   lon_var='XLONG', time_var='Times'):
    """
    :param boundaries: list [lat_min, lat_max, lon_min, lon_max]
    :param nc_f:
    :param points_array: multi dim array (np structured array)  with a row [name, lon, lat]
    :param rf_var_list:
    :param lat_var:
    :param lon_var:
    :param time_var:
    :return: np structured array with [(time, name1, name2, .... )]
    """

    if rf_var_list is None:
        rf_var_list = ['RAINC', 'RAINNC']

    if boundaries is None:
        lat_min = np.min(points_array[points_array.dtype.names[2]])
        lat_max = np.max(points_array[points_array.dtype.names[2]])
        lon_min = np.min(points_array[points_array.dtype.names[1]])
        lon_max = np.max(points_array[points_array.dtype.names[1]])
    else:
        lat_min, lat_max, lon_min, lon_max = boundaries

    variables = extract_variables(nc_f, rf_var_list, lat_min, lat_max, lon_min, lon_max, lat_var, lon_var, time_var)

    prcp = variables[rf_var_list[0]]
    for i in range(1, len(rf_var_list)):
        prcp = prcp + variables[rf_var_list[i]]

    diff = get_two_element_average(prcp, return_diff=True)

    result = np.array([utils.datetime_utc_to_lk(dt.datetime.strptime(t, '%Y-%m-%d_%H:%M:%S'), shift_mins=30).strftime(
        '%Y-%m-%d %H:%M:%S').encode('utf-8') for t in variables[time_var][:-1]], dtype=np.dtype([(time_var, 'U19')]))

    for p in points_array:
        lat_start_idx = np.argmin(abs(variables['XLAT'] - p[2]))
        lon_start_idx = np.argmin(abs(variables['XLONG'] - p[1]))
        rf = np.round(diff[:, lat_start_idx, lon_start_idx], 6)
        # use this for 4 point average
        # rf = np.round(np.mean(diff[:, lat_start_idx:lat_start_idx + 2, lon_start_idx:lon_start_idx + 2], axis=(1, 2)),
        #               6)
        result = append_fields(result, p[0].decode(), rf, usemask=False)

    return result


def extract_variables(nc_f, var_list, lat_min, lat_max, lon_min, lon_max, lat_var='XLAT', lon_var='XLONG',
                      time_var='Times'):
    """
    extract variables from a netcdf file
    :param nc_f: 
    :param var_list: comma separated string for variables / list of strings 
    :param lat_min: 
    :param lat_max: 
    :param lon_min: 
    :param lon_max: 
    :param lat_var: 
    :param lon_var: 
    :param time_var: 
    :return: 
    variables dict {var_key --> var[time, lat, lon], xlat --> [lat], xlong --> [lon], times --> [time]}
    """
    if not os.path.exists(nc_f):
        raise IOError('File %s not found' % nc_f)

    nc_fid = Dataset(nc_f, 'r')

    times = np.array([''.join([y.decode() for y in x]) for x in nc_fid.variables[time_var][:]])
    lats = nc_fid.variables[lat_var][0, :, 0]
    lons = nc_fid.variables[lon_var][0, 0, :]

    lat_inds = np.where((lats >= lat_min) & (lats <= lat_max))
    lon_inds = np.where((lons >= lon_min) & (lons <= lon_max))

    vars_dict = {}
    if isinstance(var_list, str):
        var_list = var_list.replace(',', ' ').split()
    # var_list = var_list.replace(',', ' ').split() if isinstance(var_list, str) else var_list
    for var in var_list:
        vars_dict[var] = nc_fid.variables[var][:, lat_inds[0], lon_inds[0]]

    nc_fid.close()

    vars_dict[time_var] = times
    vars_dict[lat_var] = lats[lat_inds[0]]
    vars_dict[lon_var] = lons[lon_inds[0]]

    # todo: implement this archiving procedure
    # if output is not None:
    #     logging.info('%s will be archied to %s' % (nc_f, output))
    #     ncks_extract_variables(nc_f, var_str, output)

    return vars_dict


def ncks_extract_variables(nc_file, variables, dest):
    v = ','.join(variables)
    logging.info('ncks extraction of %s for %s vars to %s' % (nc_file, v, dest))
    ncks_query = 'ncks -v %s %s %s' % (v, nc_file, dest)
    utils.run_subprocess(ncks_query)


def create_asc_file(data, lats, lons, out_file_path, cell_size=0.1, no_data_val=-99, overwrite=False):
    if not utils.file_exists_nonempty(out_file_path) or overwrite:
        with open(out_file_path, 'wb') as out_file:
            out_file.write(('NCOLS %d\n' % len(lons)).encode())
            out_file.write(('NROWS %d\n' % len(lats)).encode())
            out_file.write(('XLLCORNER %f\n' % lons[0]).encode())
            out_file.write(('YLLCORNER %f\n' % lats[0]).encode())
            out_file.write(('CELLSIZE %f\n' % cell_size).encode())
            out_file.write(('NODATA_VALUE %d\n' % no_data_val).encode())

            np.savetxt(out_file, data, fmt='%.4f')
    else:
        logging.info('%s already exits' % out_file_path)


def read_asc_file(path):
    """
    reads a esri asci file 
    :param path: file path
    :return: (data, meta data)
    """
    meta = {}
    with open(path) as f:
        for i in range(6):
            line = next(f).split()
            meta[line[0]] = float(line[1])

    data = np.genfromtxt(path, skip_header=6)
    return data, meta


def create_contour_plot(data, out_file_path, lat_min, lon_min, lat_max, lon_max, plot_title, basemap=None, clevs=None,
                        cmap=plt.get_cmap('Reds'), overwrite=False, norm=None, additional_changes=None, **kwargs):
    """
    create a contour plot using basemap
    :param additional_changes:
    :param norm:
    :param cmap: color map
    :param clevs: color levels
    :param basemap: creating basemap takes time, hence you can create it outside and pass it over
    :param plot_title:
    :param data: 2D grid data
    :param out_file_path:
    :param lat_min:
    :param lon_min:
    :param lat_max:
    :param lon_max:
    :param overwrite:
    :return:
    """
    if not utils.file_exists_nonempty(out_file_path) or overwrite:
        fig = plt.figure(figsize=(8.27, 11.69))
        # ax = fig.add_axes([0.1, 0.1, 0.8, 0.8])
        if basemap is None:
            basemap = Basemap(projection='merc', llcrnrlon=lon_min, llcrnrlat=lat_min, urcrnrlon=lon_max,
                              urcrnrlat=lat_max,
                              resolution='h')
        basemap.drawcoastlines()
        parallels = np.arange(math.floor(lat_min) - 1, math.ceil(lat_max) + 1, 1)
        basemap.drawparallels(parallels, labels=[1, 0, 0, 0], fontsize=10)
        meridians = np.arange(math.floor(lon_min) - 1, math.ceil(lon_max) + 1, 1)
        basemap.drawmeridians(meridians, labels=[0, 0, 0, 1], fontsize=10)

        ny = data.shape[0]
        nx = data.shape[1]
        lons, lats = basemap.makegrid(nx, ny)

        if clevs is None:
            clevs = np.arange(-1, np.max(data) + 1, 1)

        # cs = basemap.contourf(lons, lats, data, clevs, cmap=cm.s3pcpn_l, latlon=True)
        cs = basemap.contourf(lons, lats, data, clevs, cmap=cmap, latlon=True, norm=norm)

        cbar = basemap.colorbar(cs, location='bottom', pad="5%")
        cbar.set_label('mm')

        if isinstance(plot_title, str):
            plt.title(plot_title)
        elif isinstance(plot_title, dict):
            plt.title(plot_title.pop('label'), **plot_title)

        # make any additional changes to the plot
        if additional_changes is not None:
            additional_changes(plt, data, **kwargs)

        # draw_center_of_mass(data)
        # com = ndimage.measurements.center_of_mass(data)
        # plt.plot(com[1], com[0], 'ro')

        plt.draw()
        plt.savefig(out_file_path)
        # fig.savefig(out_file_path)
        plt.close()
    else:
        logging.info('%s already exists' % out_file_path)


def shrink_2d_array(data, new_shape, agg_func=np.average):
    """
    shrinks a 2d np array 
    :param data: np data array dim(x, y)
    :param new_shape: tuple of (row dim, col dim) ex: (x1, y1)
    :param agg_func: ex: np.sum, np.average, np.max 
    :return: array with dim (x1, y1)
    """
    cur_shape = np.shape(data)
    row_bins = np.round(np.arange(new_shape[0] + 1) * cur_shape[0] / new_shape[0]).astype(int)
    col_bins = np.round(np.arange(new_shape[1] + 1) * cur_shape[1] / new_shape[1]).astype(int)

    output = np.zeros(new_shape)
    for i in range(len(row_bins) - 1):
        for j in range(len(col_bins) - 1):
            output[i, j] = agg_func(data[row_bins[i]:row_bins[i + 1], col_bins[j]:col_bins[j + 1]])

    return output


def create_gif(filenames, output, duration=0.5):
    images = []
    for filename in filenames:
        images.append(imageio.imread(filename))
    imageio.mimsave(output, images, duration=duration)


def get_mean_cell_size(lats, lons):
    return np.round(np.mean(np.append(lons[1:len(lons)] - lons[0: len(lons) - 1], lats[1:len(lats)]
                                      - lats[0: len(lats) - 1])), 3)


def get_curw_adapter(mysql_config=None, mysql_config_path=None):
    if mysql_config_path is None:
        mysql_config_path = res_mgr.get_resource_path('config/mysql_config.json')

    with open(mysql_config_path) as data_file:
        config = json.load(data_file)

    if mysql_config is not None and isinstance(mysql_config, dict):
        config.update(mysql_config)

    return MySQLAdapter(**config)


def push_rainfall_to_db(curw_db_adapter, timeseries_dict, types=None, timesteps=24, upsert=False, source='WRF',
                        source_params='{}', name='Cloud-1'):
    if types is None:
        types = ['Forecast-0-d', 'Forecast-1-d-after', 'Forecast-2-d-after']

    if not curw_db_adapter.get_source(name=source):
        logging.info('Creating source ' + source)
        curw_db_adapter.create_source([source, source_params])

    for station, timeseries in timeseries_dict.items():
        logging.info('Pushing data for station ' + station)
        for i in range(int(np.ceil(len(timeseries) / timesteps))):
            meta_data = {
                'station': station,
                'variable': 'Precipitation',
                'unit': 'mm',
                'type': types[i],
                'source': source,
                'name': name,
            }

            event_id = curw_db_adapter.get_event_id(meta_data)
            if event_id is None:
                event_id = curw_db_adapter.create_event_id(meta_data)
                logging.debug('HASH SHA256 created: ' + event_id)

            row_count = curw_db_adapter.insert_timeseries(event_id, timeseries[i * timesteps:(i + 1) * timesteps],
                                                          upsert=upsert)
            logging.info('%d rows inserted' % row_count)


def create_station_if_not_exists(curw_db_adapter, station):
    """
    
    :param curw_db_adapter: 
    :param station: 
    :return: true if station was created else false
    """
    q = {'name': station[1]}
    if curw_db_adapter.get_station(q) is None:
        curw_db_adapter.create_station(station)
        return True
    return False


def parse_database_data_type(d_type, adapter_pkg_name='curwmysqladapter', adapter_class_name='Data'):
    if not d_type:
        d_type = 'data'
    p = __import__(adapter_pkg_name)
    c = getattr(p, adapter_class_name)
    return getattr(c, d_type)


def extract_area_rf_series(nc_f, lat_min, lat_max, lon_min, lon_max):
    if not os.path.exists(nc_f):
        raise IOError('File %s not found' % nc_f)

    nc_fid = Dataset(nc_f, 'r')

    times_len, times = extract_time_data(nc_f)
    lats = nc_fid.variables['XLAT'][0, :, 0]
    lons = nc_fid.variables['XLONG'][0, 0, :]

    lon_min_idx = np.argmax(lons >= lon_min) - 1
    lat_min_idx = np.argmax(lats >= lat_min) - 1
    lon_max_idx = np.argmax(lons >= lon_max)
    lat_max_idx = np.argmax(lats >= lat_max)

    prcp = nc_fid.variables['RAINC'][:, lat_min_idx:lat_max_idx, lon_min_idx:lon_max_idx] + nc_fid.variables['RAINNC'][
                                                                                            :, lat_min_idx:lat_max_idx,
                                                                                            lon_min_idx:lon_max_idx]

    diff = get_two_element_average(prcp)

    nc_fid.close()

    return diff, lats[lat_min_idx:lat_max_idx], lons[lon_min_idx:lon_max_idx], np.array(times[0:times_len - 1])


class TestExtractorUtils(unittest.TestCase):
    def test_extract_point_rf_series(self):
        points = np.genfromtxt(res_mgr.get_resource_path('extraction/local/metro_col_sub_catch_centroids.txt'),
                               delimiter=',', names=True, dtype=None)

        extract_points_array_rf_series(res_mgr.get_resource_path('/test/wrfout_d03_2017-12-09_18:00:00_rf'),
                                             points)

    def test_create_contour_plot(self):
        nc = '/home/nira/Desktop/temp/wrfout_d03_2017-09-24_00-00-00_SL'
        out_dir = '/home/nira/Desktop/temp'

        lat_min = 5.722969
        lon_min = 79.52146
        lat_max = 10.06425
        lon_max = 82.18992

        clevs = 10 * np.array([0.1, 0.5, 1, 2, 3, 5, 10, 15, 20, 25, 30])
        basemap = Basemap(projection='merc', llcrnrlon=lon_min, llcrnrlat=lat_min, urcrnrlon=lon_max,
                          urcrnrlat=lat_max, resolution='h')
        norm = colors.BoundaryNorm(boundaries=clevs, ncolors=256)
        cmap = plt.get_cmap('jet')
        # cmap = cm.s3pcpn

        rf_vars = ['RAINC', 'RAINNC']

        rf_values = extract_variables(nc, rf_vars, lat_min, lat_max, lon_min, lon_max)

        rf_values['PRECIP'] = rf_values[rf_vars[0]]
        for i in range(1, len(rf_vars)):
            rf_values['PRECIP'] = rf_values['PRECIP'] + rf_values[rf_vars[i]]

        os.makedirs(out_dir, exist_ok=True)
        create_contour_plot(rf_values['PRECIP'][24], out_dir + '/out.png', lat_min, lon_min, lat_max, lon_max, 'Title',
                            basemap=basemap, clevs=clevs, cmap=cmap, overwrite=True, norm=norm)

        title_opts = {'label': 'Title', 'fontsize': 30}
        create_contour_plot(rf_values['PRECIP'][24], out_dir + '/out1.png', lat_min, lon_min, lat_max, lon_max,
                            title_opts,
                            basemap=basemap, clevs=clevs, cmap=cmap, overwrite=True, norm=norm)


if __name__ == "__main__":
    pass
