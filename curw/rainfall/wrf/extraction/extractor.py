import csv
import datetime as dt
import glob
import logging
import multiprocessing
import os
import shutil
import tempfile
import unittest
import zipfile
from random import random
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
import shapefile
from joblib import Parallel, delayed
from mpl_toolkits.basemap import Basemap, cm
from netCDF4 import Dataset

from curw.rainfall.wrf import utils
from curw.rainfall.wrf.extraction import constants, spatial_utils
from curw.rainfall.wrf.extraction import utils as ext_utils
from curw.rainfall.wrf.resources import manager as res_mgr
from curwmysqladapter import Station


def extract_metro_colombo(nc_f, wrf_output, wrf_output_base, curw_db_adapter=None, curw_db_upsert=False,
                          run_prefix='WRF', run_name='Cloud-1'):
    """
    extract Metro-Colombo rf and divide area into to 4 quadrants 
    :param wrf_output_base: 
    :param run_name: 
    :param nc_f: 
    :param wrf_output: 
    :param curw_db_adapter: If not none, data will be pushed to the db 
    :param run_prefix: 
    :param curw_db_upsert: 
    :return: 
    """
    prefix = 'met_col'
    lon_min, lat_min, lon_max, lat_max = constants.COLOMBO_EXTENT

    nc_vars = ext_utils.extract_variables(nc_f, ['RAINC', 'RAINNC'], lat_min, lat_max, lon_min, lon_max)
    lats = nc_vars['XLAT']
    lons = nc_vars['XLONG']
    prcp = nc_vars['RAINC'] + nc_vars['RAINNC']
    times = nc_vars['Times']

    diff = ext_utils.get_two_element_average(prcp)

    width = len(lons)
    height = len(lats)

    output_dir = utils.create_dir_if_not_exists(os.path.join(wrf_output, prefix))

    basin_rf = np.mean(diff[0:(len(times) - 1 if len(times) < 24 else 24), :, :])

    alpha_file_path = os.path.join(wrf_output_base, prefix + '_alphas.txt')
    utils.create_dir_if_not_exists(os.path.dirname(alpha_file_path))
    with open(alpha_file_path, 'a+') as alpha_file:
        t = utils.datetime_utc_to_lk(dt.datetime.strptime(times[0], '%Y-%m-%d_%H:%M:%S'), shift_mins=30)
        alpha_file.write('%s\t%f\n' % (t.strftime('%Y-%m-%d_%H:%M:%S'), basin_rf))

    cz = ext_utils.get_mean_cell_size(lats, lons)
    no_data = -99

    divs = (2, 2)
    div_rf = {}
    for i in range(divs[0] * divs[1]):
        div_rf[prefix + str(i)] = []

    with TemporaryDirectory(prefix=prefix) as temp_dir:
        subsection_file_path = os.path.join(temp_dir, 'sub_means.txt')
        with open(subsection_file_path, 'w') as subsection_file:
            for tm in range(0, len(times) - 1):
                t_str = (
                    utils.datetime_utc_to_lk(dt.datetime.strptime(times[tm], '%Y-%m-%d_%H:%M:%S'),
                                             shift_mins=30)).strftime('%Y-%m-%d %H:%M:%S')

                output_file_path = os.path.join(temp_dir, 'rf_' + t_str.replace(' ', '_') + '.asc')
                ext_utils.create_asc_file(np.flip(diff[tm, :, :], 0), lats, lons, output_file_path, cell_size=cz,
                                          no_data_val=no_data)

                # writing subsection file
                x_idx = [round(i * width / divs[0]) for i in range(0, divs[0] + 1)]
                y_idx = [round(i * height / divs[1]) for i in range(0, divs[1] + 1)]

                subsection_file.write(t_str)
                for j in range(len(y_idx) - 1):
                    for i in range(len(x_idx) - 1):
                        quad = j * divs[1] + i
                        sub_sec_mean = np.mean(diff[tm, y_idx[j]:y_idx[j + 1], x_idx[i]: x_idx[i + 1]])
                        subsection_file.write('\t%.4f' % sub_sec_mean)
                        div_rf[prefix + str(quad)].append([t_str, sub_sec_mean])
                subsection_file.write('\n')

        utils.create_zip_with_prefix(temp_dir, 'rf_*.asc', os.path.join(temp_dir, 'ascs.zip'), clean_up=True)

        utils.move_files_with_prefix(temp_dir, '*', output_dir)

    # writing to the database
    if curw_db_adapter is not None:
        for i in range(divs[0] * divs[1]):
            name = prefix + str(i)
            station = [Station.CUrW, name, name, -999, -999, 0, "met col quadrant %d" % i]
            if ext_utils.create_station_if_not_exists(curw_db_adapter, station):
                logging.info('%s station created' % name)

        logging.info('Pushing data to the db...')
        ext_utils.push_rainfall_to_db(curw_db_adapter, div_rf, upsert=curw_db_upsert, source=run_prefix, name=run_name)
    else:
        logging.info('curw_db_adapter not available. Unable to push data!')

    return basin_rf


def extract_weather_stations(nc_f, wrf_output, weather_stations=None, curw_db_adapter=None, curw_db_upsert=False,
                             run_prefix='WRF', run_name='Cloud-1'):
    if weather_stations is None:
        weather_stations = res_mgr.get_resource_path('extraction/local/kelani_basin_stations.txt')

    nc_fid = Dataset(nc_f, 'r')
    times_len, times = ext_utils.extract_time_data(nc_f)

    prefix = 'stations_rf'
    stations_dir = utils.create_dir_if_not_exists(os.path.join(wrf_output, prefix))

    stations_rf = {}
    with TemporaryDirectory(prefix=prefix) as temp_dir:
        with open(weather_stations, 'r') as csvfile:
            stations = csv.reader(csvfile, delimiter=' ')

            for row in stations:
                logging.info(' '.join(row))
                lon = row[1]
                lat = row[2]

                station_prcp = nc_fid.variables['RAINC'][:, lat, lon] + nc_fid.variables['RAINNC'][:, lat, lon]

                station_diff = ext_utils.get_two_element_average(station_prcp)

                stations_rf[row[0]] = []

                station_file_path = os.path.join(temp_dir, row[0] + '_%s.txt' % prefix)
                with open(station_file_path, 'w') as station_file:
                    for t in range(0, len(times) - 1):
                        t_str = (
                            utils.datetime_utc_to_lk(dt.datetime.strptime(times[t], '%Y-%m-%d_%H:%M:%S'),
                                                     shift_mins=30)).strftime('%Y-%m-%d %H:%M:%S')
                        station_file.write('%s\t%.4f\n' % (t_str, station_diff[t]))
                        stations_rf[row[0]].append([t_str, station_diff[t]])

        utils.move_files_with_prefix(temp_dir, '*.txt', stations_dir)

    if curw_db_adapter is not None:
        logging.info('Pushing data to the db...')
        ext_utils.push_rainfall_to_db(curw_db_adapter, stations_rf, upsert=curw_db_upsert, name=run_name,
                                      source=run_prefix)
    else:
        logging.info('curw_db_adapter not available. Unable to push data!')

    nc_fid.close()


def extract_weather_stations2(nc_f, wrf_output, weather_stations=None, curw_db_adapter=None, curw_db_upsert=False,
                              run_prefix='WRF', run_name='Cloud-1'):
    if weather_stations is None:
        weather_stations = res_mgr.get_resource_path('extraction/local/wrf_stations.txt')

    points = np.genfromtxt(weather_stations, delimiter=',', names=True, dtype=None)

    point_prcp = ext_utils.extract_points_array_rf_series(nc_f, points)

    t0 = dt.datetime.strptime(point_prcp['Times'][0], '%Y-%m-%d %H:%M:%S')
    t1 = dt.datetime.strptime(point_prcp['Times'][1], '%Y-%m-%d %H:%M:%S')

    res_min = int((t1 - t0).total_seconds() / 60)

    prefix = 'stations_rf'
    stations_dir = utils.create_dir_if_not_exists(os.path.join(wrf_output, prefix))

    stations_rf = {}
    with TemporaryDirectory(prefix=prefix) as temp_dir:
        for point in points:
            logging.info(str(point))
            station_name = point[0].decode()
            stations_rf[station_name] = []

            station_file_path = os.path.join(temp_dir, station_name + '_%s.txt' % prefix)
            with open(station_file_path, 'w') as station_file:
                for t in range(0, len(point_prcp)):
                    station_file.write('%s\t%.4f\n' % (point_prcp['Times'][t], point_prcp[station_name][t]))
                    stations_rf[station_name].append([point_prcp['Times'][t], point_prcp[station_name][t]])

        utils.move_files_with_prefix(temp_dir, '*.txt', stations_dir)

    if curw_db_adapter is not None:
        logging.info('Pushing data to the db...')
        ext_utils.push_rainfall_to_db(curw_db_adapter, stations_rf, upsert=curw_db_upsert, name=run_name,
                                      source=run_prefix)
    else:
        logging.info('curw_db_adapter not available. Unable to push data!')


def extract_kelani_basin_rainfall_flo2d(nc_f, nc_f_prev_days, output_dir, avg_basin_rf=1.0, kelani_basin_file=None,
                                        target_rfs=None, output_prefix='RAINCELL'):
    """
    :param output_prefix:
    :param nc_f:
    :param nc_f_prev_days: 
    :param output_dir: 
    :param avg_basin_rf: 
    :param kelani_basin_file: 
    :param target_rfs: 
    :return: 
    """
    if target_rfs is None:
        target_rfs = [100, 150, 200, 250, 300]
    if kelani_basin_file is None:
        kelani_basin_file = res_mgr.get_resource_path('extraction/local/kelani_basin_points_250m.txt')

    points = np.genfromtxt(kelani_basin_file, delimiter=',')

    kel_lon_min = np.min(points, 0)[1]
    kel_lat_min = np.min(points, 0)[2]
    kel_lon_max = np.max(points, 0)[1]
    kel_lat_max = np.max(points, 0)[2]

    diff, kel_lats, kel_lons, times = ext_utils.extract_area_rf_series(nc_f, kel_lat_min, kel_lat_max, kel_lon_min,
                                                                       kel_lon_max)

    def get_bins(arr):
        sz = len(arr)
        return (arr[1:sz - 1] + arr[0:sz - 2]) / 2

    lat_bins = get_bins(kel_lats)
    lon_bins = get_bins(kel_lons)

    t0 = dt.datetime.strptime(times[0], '%Y-%m-%d_%H:%M:%S')
    t1 = dt.datetime.strptime(times[1], '%Y-%m-%d_%H:%M:%S')
    t_end = dt.datetime.strptime(times[-1], '%Y-%m-%d_%H:%M:%S')

    utils.create_dir_if_not_exists(output_dir)

    prev_diff = []
    prev_days = len(nc_f_prev_days)
    for i in range(prev_days):
        if nc_f_prev_days[i]:
            p_diff, _, _, _ = ext_utils.extract_area_rf_series(nc_f_prev_days[i], kel_lat_min, kel_lat_max, kel_lon_min,
                                                               kel_lon_max)
            prev_diff.append(p_diff)
        else:
            prev_diff.append(None)

    def write_forecast_to_raincell_file(output_file_path, alpha):
        with open(output_file_path, 'w') as output_file:
            res_mins = int((t1 - t0).total_seconds() / 60)
            data_hours = int(len(times) + prev_days * 24 * 60 / res_mins)
            start_ts = utils.datetime_utc_to_lk(t0 - dt.timedelta(days=prev_days), shift_mins=30).strftime(
                '%Y-%m-%d %H:%M:%S')
            end_ts = utils.datetime_utc_to_lk(t_end, shift_mins=30).strftime('%Y-%m-%d %H:%M:%S')

            output_file.write("%d %d %s %s\n" % (res_mins, data_hours, start_ts, end_ts))

            for d in range(prev_days):
                for t in range(int(24 * 60 / res_mins)):
                    for point in points:
                        rf_x = np.digitize(point[1], lon_bins)
                        rf_y = np.digitize(point[2], lat_bins)
                        if prev_diff[prev_days - 1 - d] is not None:
                            output_file.write('%d %.1f\n' % (point[0], prev_diff[prev_days - 1 - d][t, rf_y, rf_x]))
                        else:
                            output_file.write('%d %.1f\n' % (point[0], 0))

            for t in range(len(times)):
                for point in points:
                    rf_x = np.digitize(point[1], lon_bins)
                    rf_y = np.digitize(point[2], lat_bins)
                    if t < int(24 * 60 / res_mins):
                        output_file.write('%d %.1f\n' % (point[0], diff[t, rf_y, rf_x] * alpha))
                    else:
                        output_file.write('%d %.1f\n' % (point[0], diff[t, rf_y, rf_x]))

    with TemporaryDirectory(prefix='curw_raincell') as temp_dir:
        raincell_temp = os.path.join(temp_dir, output_prefix + '.DAT')
        write_forecast_to_raincell_file(raincell_temp, 1)

        for target_rf in target_rfs:
            write_forecast_to_raincell_file('%s.%d' % (raincell_temp, target_rf), target_rf / avg_basin_rf)

        utils.create_zip_with_prefix(temp_dir, output_prefix + '.DAT*', os.path.join(temp_dir, output_prefix + '.zip'),
                                     clean_up=True)
        utils.move_files_with_prefix(temp_dir, output_prefix + '.zip', utils.create_dir_if_not_exists(output_dir))


class CurwExtractorException(Exception):
    pass


def create_rainfall_for_mike21(d0_rf_file, prev_rf_files, output_dir):
    d0 = np.genfromtxt(d0_rf_file, dtype=str)

    t0 = dt.datetime.strptime(' '.join(d0[0][:-1]), '%Y-%m-%d %H:%M:%S')
    t1 = dt.datetime.strptime(' '.join(d0[1][:-1]), '%Y-%m-%d %H:%M:%S')

    res_min = int((t1 - t0).total_seconds() / 60)
    lines_per_day = int(24 * 60 / res_min)
    prev_days = len(prev_rf_files)

    output = None
    for i in range(prev_days):
        if prev_rf_files[prev_days - 1 - i] is not None:
            if output is not None:
                output = np.append(output,
                                   np.genfromtxt(prev_rf_files[prev_days - 1 - i], dtype=str, max_rows=lines_per_day),
                                   axis=0)
            else:
                output = np.genfromtxt(prev_rf_files[prev_days - 1 - i], dtype=str, max_rows=lines_per_day)
        else:
            output = None  # if any of the previous files are missing, skip prepending past data to the forecast
            break

    if output is not None:
        output = np.append(output, d0, axis=0)
    else:
        output = d0

    out_file = os.path.join(utils.create_dir_if_not_exists(output_dir), 'rf_mike21.txt')
    with open(out_file, 'w') as out_f:
        for line in output:
            out_f.write('%s %s\t%s\n' % (line[0], line[1], line[2]))


def extract_metro_col_rf_for_mike21(nc_f, output_dir, prev_rf_files=None, points_file=None):
    if not prev_rf_files:
        prev_rf_files = []

    if not points_file:
        points_file = res_mgr.get_resource_path('extraction/local/metro_col_sub_catch_centroids.txt')
    points = np.genfromtxt(points_file, delimiter=',', names=True, dtype=None)

    point_prcp = ext_utils.extract_points_array_rf_series(nc_f, points)

    t0 = dt.datetime.strptime(point_prcp['Times'][0], '%Y-%m-%d %H:%M:%S')
    t1 = dt.datetime.strptime(point_prcp['Times'][1], '%Y-%m-%d %H:%M:%S')

    res_min = int((t1 - t0).total_seconds() / 60)
    lines_per_day = int(24 * 60 / res_min)
    prev_days = len(prev_rf_files)

    output = None
    for i in range(prev_days):
        if prev_rf_files[prev_days - 1 - i] is not None:
            if output is not None:
                output = np.append(output,
                                   ext_utils.extract_points_array_rf_series(prev_rf_files[prev_days - 1 - i], points)[
                                   :lines_per_day], axis=0)
            else:
                output = ext_utils.extract_points_array_rf_series(prev_rf_files[prev_days - 1 - i], points)[
                         :lines_per_day]
        else:
            output = None  # if any of the previous files are missing, skip prepending past data to the forecast
            break

    if output is not None:
        output = np.append(output, point_prcp, axis=0)
    else:
        output = point_prcp

    fmt = '%s'
    for _ in range(len(output[0]) - 1):
        fmt = fmt + ',%g'
    header = ','.join(output.dtype.names)

    utils.create_dir_if_not_exists(output_dir)
    np.savetxt(os.path.join(output_dir, 'met_col_rf_mike21.txt'), output, fmt=fmt, delimiter=',', header=header,
               comments='', encoding='utf-8')


def extract_mean_rainfall_from_shp_file(nc_f, wrf_output, output_prefix, output_name, basin_shp_file, basin_extent,
                                        curw_db_adapter=None, curw_db_upsert=False, run_prefix='WRF',
                                        run_name='Cloud-1'):
    lon_min, lat_min, lon_max, lat_max = basin_extent

    nc_vars = ext_utils.extract_variables(nc_f, ['RAINC', 'RAINNC'], lat_min, lat_max, lon_min, lon_max)
    lats = nc_vars['XLAT']
    lons = nc_vars['XLONG']
    prcp = nc_vars['RAINC'] + nc_vars['RAINNC']
    times = nc_vars['Times']

    diff = ext_utils.get_two_element_average(prcp)

    polys = shapefile.Reader(basin_shp_file)

    output_dir = utils.create_dir_if_not_exists(os.path.join(wrf_output, output_prefix))

    with TemporaryDirectory(prefix=output_prefix) as temp_dir:
        output_file_path = os.path.join(temp_dir, output_prefix + '.txt')
        kub_rf = {}
        with open(output_file_path, 'w') as output_file:
            kub_rf[output_name] = []
            for t in range(0, len(times) - 1):
                cnt = 0
                rf_sum = 0.0
                for y in range(0, len(lats)):
                    for x in range(0, len(lons)):
                        if utils.is_inside_polygon(polys, lats[y], lons[x]):
                            cnt = cnt + 1
                            rf_sum = rf_sum + diff[t, y, x]
                mean_rf = rf_sum / cnt

                t_str = (
                    utils.datetime_utc_to_lk(dt.datetime.strptime(times[t], '%Y-%m-%d_%H:%M:%S'),
                                             shift_mins=30)).strftime('%Y-%m-%d %H:%M:%S')
                output_file.write('%s\t%.4f\n' % (t_str, mean_rf))
                kub_rf[output_name].append([t_str, mean_rf])

        utils.move_files_with_prefix(temp_dir, '*.txt', output_dir)

    if curw_db_adapter is not None:
        station = [Station.CUrW, output_name, output_name, -999, -999, 0, 'Kelani upper basin mean rainfall']
        if ext_utils.create_station_if_not_exists(curw_db_adapter, station):
            logging.info('%s station created' % output_name)

        logging.info('Pushing data to the db...')
        ext_utils.push_rainfall_to_db(curw_db_adapter, kub_rf, upsert=curw_db_upsert, name=run_name,
                                      source=run_prefix)
    else:
        logging.info('curw_db_adapter not available. Unable to push data!')


def extract_point_rf_series(nc_f, lat, lon):
    nc_fid = Dataset(nc_f, 'r')

    times_len, times = ext_utils.extract_time_data(nc_f)
    lats = nc_fid.variables['XLAT'][0, :, 0]
    lons = nc_fid.variables['XLONG'][0, 0, :]

    lat_start_idx = np.argmin(abs(lats - lat))
    lon_start_idx = np.argmin(abs(lons - lon))

    prcp = nc_fid.variables['RAINC'][:, lat_start_idx, lon_start_idx] + \
           nc_fid.variables['RAINNC'][:, lat_start_idx, lon_start_idx] + \
           nc_fid.variables['SNOWNC'][:, lat_start_idx, lon_start_idx] + \
           nc_fid.variables['GRAUPELNC'][:, lat_start_idx, lon_start_idx]

    diff = ext_utils.get_two_element_average(prcp)

    nc_fid.close()

    return diff, np.array(times[0:times_len - 1])


def extract_jaxa_weather_stations(nc_f, weather_stations_file, output_dir):
    nc_fid = Dataset(nc_f, 'r')

    stations = pd.read_csv(weather_stations_file, header=0, sep=',')

    output_file_dir = os.path.join(output_dir, 'jaxa-stations-wrf-forecast')
    utils.create_dir_if_not_exists(output_file_dir)

    for idx, station in stations.iterrows():
        logging.info('Extracting station ' + str(station))

        rf, times = extract_point_rf_series(nc_f, station[2], station[1])

        output_file_path = os.path.join(output_file_dir,
                                        station[3] + '-' + str(station[0]) + '-' + times[0].split('_')[0] + '.txt')
        output_file = open(output_file_path, 'w')
        output_file.write('jaxa-stations-wrf-forecast\n')
        output_file.write(', '.join(stations.columns.values) + '\n')
        output_file.write(', '.join(str(x) for x in station) + '\n')
        output_file.write('timestamp, rainfall\n')
        for i in range(len(times)):
            output_file.write('%s, %f\n' % (times[i], rf[i]))
        output_file.close()

    nc_fid.close()


def extract_jaxa_satellite_data(start_ts_utc, end_ts_utc, output_dir):
    start = utils.datetime_floor(start_ts_utc, 3600)
    end = utils.datetime_floor(end_ts_utc, 3600)

    lat_min = 5.722969
    lon_min = 79.52146
    lat_max = 10.06425
    lon_max = 82.18992

    login = 'rainmap:Niskur+1404'

    url0 = 'ftp://' + login + '@hokusai.eorc.jaxa.jp/realtime/txt/05_AsiaSS/YYYY/MM/DD/gsmap_nrt.YYYYMMDD.HH00.05_AsiaSS.csv.zip'
    url1 = 'ftp://' + login + '@hokusai.eorc.jaxa.jp/now/txt/05_AsiaSS/gsmap_now.YYYYMMDD.HH00_HH59.05_AsiaSS.csv.zip'

    def get_jaxa_url(ts):
        url_switch = (dt.datetime.utcnow() - ts) > dt.timedelta(hours=5)
        _url = url0 if url_switch else url1
        ph = {'YYYY': ts.strftime('%Y'),
              'MM': ts.strftime('%m'),
              'DD': ts.strftime('%d'),
              'HH': ts.strftime('%H')}
        for k, v in ph.iteritems():
            _url = _url.replace(k, v)
        return _url

    tmp_dir = os.path.join(output_dir, 'tmp_jaxa/')
    if not os.path.exists(tmp_dir):
        os.mkdir(tmp_dir)
    else:
        utils.cleanup_dir(tmp_dir)

    url_dest_list = []
    for timestamp in np.arange(start, end, dt.timedelta(hours=1)).astype(dt.datetime):
        url = get_jaxa_url(timestamp)
        url_dest_list.append((url, os.path.join(tmp_dir, os.path.basename(url)),
                              os.path.join(output_dir, 'jaxa_sat_rf_' + timestamp.strftime('%Y-%m-%d_%H:%M') + '.asc')))

    utils.download_parallel(url_dest_list)

    procs = multiprocessing.cpu_count()
    Parallel(n_jobs=procs)(
        delayed(_process_zip_file)(i[1], i[2], lat_min, lon_min, lat_max, lon_max) for i in url_dest_list)

    # clean up temp dir
    shutil.rmtree(tmp_dir)


def _process_zip_file(zip_file_path, out_file_path, lat_min, lon_min, lat_max, lon_max):
    sat_zip = zipfile.ZipFile(zip_file_path)
    sat = np.genfromtxt(sat_zip.open(os.path.basename(zip_file_path).replace('.zip', '')), delimiter=',', names=True)
    sat_filt = sat[
        (sat['Lat'] <= lat_max) & (sat['Lat'] >= lat_min) & (sat['Lon'] <= lon_max) & (sat['Lon'] >= lon_min)]
    lats = np.sort(np.unique(sat_filt['Lat']))
    lons = np.sort(np.unique(sat_filt['Lon']))

    cell_size = 0.1
    no_data_val = -99
    out_file = open(out_file_path, 'w')
    out_file.write('NCOLS %d\n' % len(lons))
    out_file.write('NROWS %d\n' % len(lats))
    out_file.write('XLLCORNER %f\n' % lons[0])
    out_file.write('YLLCORNER %f\n' % lats[0])
    out_file.write('CELLSIZE %f\n' % cell_size)
    out_file.write('NODATA_VALUE %d\n' % no_data_val)

    for lat in np.flip(lats, 0):
        for lon in lons:
            out_file.write(str(sat[(sat['Lat'] == lat) & (sat['Lon'] == lon)][0][2]) + ' ')
        out_file.write('\n')

    out_file.close()


def push_wrf_rainfall_to_db(nc_f, curw_db_adapter=None, lon_min=None, lat_min=None, lon_max=None,
                            lat_max=None, run_prefix='WRF', upsert=False, run_name='Cloud-1', station_prefix='wrf'):
    """

    :param run_name: 
    :param nc_f:
    :param curw_db_adapter: If not none, data will be pushed to the db
    :param run_prefix:
    :param lon_min:
    :param lat_min:
    :param lon_max:
    :param lat_max:
    :param upsert: 
    :return:
    """
    if curw_db_adapter is None:
        logging.info('curw_db_adapter not available. Unable to push data!')
        return

    if not all([lon_min, lat_min, lon_max, lat_max]):
        lon_min, lat_min, lon_max, lat_max = constants.SRI_LANKA_EXTENT

    nc_vars = ext_utils.extract_variables(nc_f, ['RAINC', 'RAINNC'], lat_min, lat_max, lon_min, lon_max)
    lats = nc_vars['XLAT']
    lons = nc_vars['XLONG']
    prcp = nc_vars['RAINC'] + nc_vars['RAINNC']
    times = nc_vars['Times']

    diff = ext_utils.get_two_element_average(prcp)

    width = len(lons)
    height = len(lats)

    def random_check_stations_exist():
        for _ in range(10):
            _x = lons[int(random() * width)]
            _y = lats[int(random() * height)]
            _name = '%s_%.6f_%.6f' % (station_prefix, _x, _y)
            _query = {'name': _name}
            if curw_db_adapter.get_station(_query) is None:
                logging.debug('Random stations check fail')
                return False
        logging.debug('Random stations check success')
        return True

    stations_exists = random_check_stations_exist()

    rf_ts = {}
    for y in range(height):
        for x in range(width):
            lat = lats[y]
            lon = lons[x]

            station_id = '%s_%.6f_%.6f' % (station_prefix, lon, lat)
            name = station_id

            if not stations_exists:
                logging.info('Creating station %s ...' % name)
                station = [Station.WRF, station_id, name, str(lon), str(lat), str(0), "WRF point"]
                curw_db_adapter.create_station(station)

            # add rf series to the dict
            ts = []
            for i in range(len(diff)):
                t = utils.datetime_utc_to_lk(dt.datetime.strptime(times[i], '%Y-%m-%d_%H:%M:%S'), shift_mins=30)
                ts.append([t.strftime('%Y-%m-%d %H:%M:%S'), diff[i, y, x]])
            rf_ts[name] = ts

    ext_utils.push_rainfall_to_db(curw_db_adapter, rf_ts, source=run_prefix, upsert=upsert, name=run_name)


def create_rf_plots_wrf(nc_f, plots_output_dir, plots_output_base_dir, lon_min=None, lat_min=None, lon_max=None,
                        lat_max=None, filter_threshold=0.05, run_prefix='WRF'):
    if not all([lon_min, lat_min, lon_max, lat_max]):
        lon_min, lat_min, lon_max, lat_max = constants.SRI_LANKA_EXTENT

    variables = ext_utils.extract_variables(nc_f, 'RAINC, RAINNC', lat_min, lat_max, lon_min, lon_max)

    lats = variables['XLAT']
    lons = variables['XLONG']

    # cell size is calc based on the mean between the lat and lon points
    cz = np.round(np.mean(np.append(lons[1:len(lons)] - lons[0: len(lons) - 1], lats[1:len(lats)]
                                    - lats[0: len(lats) - 1])), 3)
    clevs = [0, 1, 2.5, 5, 7.5, 10, 15, 20, 30, 40, 50, 70, 100, 150, 200, 250, 300, 400, 500, 600, 750]
    cmap = cm.s3pcpn

    basemap = Basemap(projection='merc', llcrnrlon=lon_min, llcrnrlat=lat_min, urcrnrlon=lon_max,
                      urcrnrlat=lat_max, resolution='h')

    data = variables['RAINC'] + variables['RAINNC']
    logging.info('Filtering with the threshold %f' % filter_threshold)
    data[data < filter_threshold] = 0.0
    variables['PRECIP'] = data

    prefix = 'wrf_plots'
    with TemporaryDirectory(prefix=prefix) as temp_dir:
        t0 = dt.datetime.strptime(variables['Times'][0], '%Y-%m-%d_%H:%M:%S')
        t1 = dt.datetime.strptime(variables['Times'][1], '%Y-%m-%d_%H:%M:%S')
        step = (t1 - t0).total_seconds() / 3600.0

        inst_precip = ext_utils.get_two_element_average(variables['PRECIP'])
        cum_precip = ext_utils.get_two_element_average(variables['PRECIP'], return_diff=False)

        for i in range(1, len(variables['Times'])):
            time = variables['Times'][i]
            ts = dt.datetime.strptime(time, '%Y-%m-%d_%H:%M:%S')
            lk_ts = utils.datetime_utc_to_lk(ts, shift_mins=30)
            logging.info('processing %s', time)

            # instantaneous precipitation (hourly)
            inst_file = os.path.join(temp_dir, 'wrf_inst_' + lk_ts.strftime('%Y-%m-%d_%H:%M:%S'))

            ext_utils.create_asc_file(np.flip(inst_precip[i - 1], 0), lats, lons, inst_file + '.asc', cell_size=cz)

            title = {
                'label': 'Hourly rf for %s LK' % lk_ts.strftime('%Y-%m-%d_%H:%M:%S'),
                'fontsize': 30
            }
            ext_utils.create_contour_plot(inst_precip[i - 1], inst_file + '.png', lat_min, lon_min, lat_max, lon_max,
                                          title, clevs=clevs, cmap=cmap, basemap=basemap)

            if (i * step) % 24 == 0:
                t = 'Daily rf from %s LK to %s LK' % (
                    (lk_ts - dt.timedelta(hours=24)).strftime('%Y-%m-%d_%H:%M:%S'), lk_ts.strftime('%Y-%m-%d_%H:%M:%S'))
                d = int(i * step / 24) - 1
                logging.info('Creating images for D%d' % d)
                cum_file = os.path.join(temp_dir, 'wrf_cum_%dd' % d)

                if i * step / 24 > 1:
                    cum_precip_24h = cum_precip[i - 1] - cum_precip[i - 1 - int(24 / step)]
                else:
                    cum_precip_24h = cum_precip[i - 1]

                ext_utils.create_asc_file(np.flip(cum_precip_24h, 0), lats, lons, cum_file + '.asc', cell_size=cz)

                ext_utils.create_contour_plot(cum_precip_24h, cum_file + '.png', lat_min, lon_min, lat_max, lon_max, t,
                                              clevs=clevs, cmap=cmap, basemap=basemap)

                gif_file = os.path.join(temp_dir, 'wrf_inst_%dd' % d)
                images = [os.path.join(temp_dir, 'wrf_inst_' + j.strftime('%Y-%m-%d_%H:%M:%S') + '.png') for j in
                          np.arange(lk_ts - dt.timedelta(hours=24 - step), lk_ts + dt.timedelta(hours=step),
                                    dt.timedelta(hours=step)).astype(dt.datetime)]
                ext_utils.create_gif(images, gif_file + '.gif')

        logging.info('Creating the zips')
        utils.create_zip_with_prefix(temp_dir, '*.png', os.path.join(temp_dir, 'pngs.zip'))
        utils.create_zip_with_prefix(temp_dir, '*.asc', os.path.join(temp_dir, 'ascs.zip'))

        logging.info('Cleaning up instantaneous pngs and ascs - wrf_inst_*')
        utils.delete_files_with_prefix(temp_dir, 'wrf_inst_*.png')
        utils.delete_files_with_prefix(temp_dir, 'wrf_inst_*.asc')

        logging.info('Copying pngs to ' + plots_output_dir)
        utils.move_files_with_prefix(temp_dir, '*.png', plots_output_dir)
        logging.info('Copying ascs to ' + plots_output_dir)
        utils.move_files_with_prefix(temp_dir, '*.asc', plots_output_dir)
        logging.info('Copying gifs to ' + plots_output_dir)
        utils.copy_files_with_prefix(temp_dir, '*.gif', plots_output_dir)
        logging.info('Copying zips to ' + plots_output_dir)
        utils.copy_files_with_prefix(temp_dir, '*.zip', plots_output_dir)

        plots_latest_dir = os.path.join(plots_output_base_dir, 'latest', run_prefix, os.path.basename(plots_output_dir))
        # <nfs>/latest/wrf0 .. 3
        utils.create_dir_if_not_exists(plots_latest_dir)
        # todo: this needs to be adjusted to handle the multiple runs
        logging.info('Copying gifs to ' + plots_latest_dir)
        utils.copy_files_with_prefix(temp_dir, '*.gif', plots_latest_dir)


def suite():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    s = unittest.TestSuite()
    s.addTest(TestExtractor)
    return s


class TestExtractor(unittest.TestCase):
    def test_push_wrf_rainfall_to_db(self):
        config = {
            "host": "localhost",
            "user": "test",
            "password": "password",
            "db": "testdb"
        }
        adapter = ext_utils.get_curw_adapter(mysql_config=config)

        nc_f = res_mgr.get_resource_path('test/wrfout_d03_2017-10-02_12:00:00')
        lon_min, lat_min, lon_max, lat_max = constants.KELANI_KALU_BASIN_EXTENT
        push_wrf_rainfall_to_db(nc_f, curw_db_adapter=adapter, lat_min=lat_min, lon_min=lon_min,
                                lat_max=lat_max, lon_max=lon_max, upsert=True)

    def test_extract_kelani_basin_rainfall_flo2d(self):
        wrf_output_dir = tempfile.mkdtemp(prefix='flo2d_')
        files = ['wrfout_d03_2017-12-09_18:00:00_rf', 'wrfout_d03_2017-12-10_18:00:00_rf',
                 'wrfout_d03_2017-12-11_18:00:00_rf']
        run_prefix = 'wrf0'

        for f in files:
            out_dir = utils.create_dir_if_not_exists(
                os.path.join(wrf_output_dir, f.replace('wrfout_d03', run_prefix).replace(':00_rf', '_0000'), 'wrf'))
            shutil.copy2(res_mgr.get_resource_path('test/%s' % f), out_dir)

        run_date = dt.datetime.strptime('2017-12-11_18:00', '%Y-%m-%d_%H:%M')
        now = '_'.join([run_prefix, run_date.strftime('%Y-%m-%d_%H:%M'), '*'])
        prev_1 = '_'.join([run_prefix, (run_date - dt.timedelta(days=1)).strftime('%Y-%m-%d_%H:%M'), '*'])
        prev_2 = '_'.join([run_prefix, (run_date - dt.timedelta(days=2)).strftime('%Y-%m-%d_%H:%M'), '*'])
        d03_nc_f = glob.glob(os.path.join(wrf_output_dir, now, 'wrf', 'wrfout_d03_*'))[0]
        d03_nc_f_prev_1 = glob.glob(os.path.join(wrf_output_dir, prev_1, 'wrf', 'wrfout_d03_*'))[0]
        d03_nc_f_prev_2 = glob.glob(os.path.join(wrf_output_dir, prev_2, 'wrf', 'wrfout_d03_*'))[0]

        kelani_basin_flo2d_file = res_mgr.get_resource_path('extraction/local/kelani_basin_points_250m.txt')
        extract_kelani_basin_rainfall_flo2d(d03_nc_f, [d03_nc_f_prev_1, d03_nc_f_prev_2],
                                            os.path.join(wrf_output_dir, now, 'klb_flo2d'),
                                            kelani_basin_file=kelani_basin_flo2d_file, )

    def test_create_rainfall_for_mike21(self):
        wrf_output_dir = tempfile.mkdtemp(prefix='mike21_')
        run_date = dt.datetime.strptime('2017-12-11_18:00', '%Y-%m-%d_%H:%M')

        basin_shp_file = res_mgr.get_resource_path('extraction/shp/klb-wgs84/klb-wgs84.shp')
        files = ['wrfout_d03_2017-12-09_18:00:00_rf', 'wrfout_d03_2017-12-10_18:00:00_rf',
                 'wrfout_d03_2017-12-11_18:00:00_rf']
        run_prefix = 'wrf0'

        for f in files:
            d03_nc_f = res_mgr.get_resource_path('test/%s' % f)
            out_dir = utils.create_dir_if_not_exists(
                os.path.join(wrf_output_dir, f.replace('wrfout_d03', run_prefix).replace(':00_rf', '_0000')))
            extract_mean_rainfall_from_shp_file(d03_nc_f, out_dir, 'klb_mean_rf', 'klb_mean',
                                                basin_shp_file, constants.KELANI_LOWER_BASIN_EXTENT)

        now = '_'.join([run_prefix, run_date.strftime('%Y-%m-%d_%H:%M'), '*'])
        prev_1 = '_'.join([run_prefix, (run_date - dt.timedelta(days=1)).strftime('%Y-%m-%d_%H:%M'), '*'])
        prev_2 = '_'.join([run_prefix, (run_date - dt.timedelta(days=2)).strftime('%Y-%m-%d_%H:%M'), '*'])
        d03_nc_f = glob.glob(os.path.join(wrf_output_dir, now, 'klb_mean_rf', 'klb_mean_rf.txt'))[0]
        d03_nc_f_prev_1 = glob.glob(os.path.join(wrf_output_dir, prev_1, 'klb_mean_rf', 'klb_mean_rf.txt'))[0]
        d03_nc_f_prev_2 = glob.glob(os.path.join(wrf_output_dir, prev_2, 'klb_mean_rf', 'klb_mean_rf.txt'))[0]

        create_rainfall_for_mike21(d03_nc_f, [d03_nc_f_prev_1, d03_nc_f_prev_2],
                                   os.path.join(wrf_output_dir, now, 'mike_21'))

    def test_extract_weather_stations(self):
        adapter = ext_utils.get_curw_adapter()
        out_dir = tempfile.mkdtemp(prefix='stations_')
        extract_weather_stations(res_mgr.get_resource_path('test/wrfout_d03_2017-10-02_12:00:00'),
                                 out_dir, curw_db_adapter=adapter, curw_db_upsert=True)

    def test_extract_weather_stations2(self):
        adapter = None
        out_dir = tempfile.mkdtemp(prefix='stations_')
        extract_weather_stations2('/home/curw/Desktop/2018-05/2018-05-22_18:00/wrf0/wrfout_d03_2018-05-22_18:00:00_rf',
                                  out_dir, curw_db_adapter=adapter, curw_db_upsert=True)

    def test_extract_metro_colombo(self):
        # adapter = ext_utils.get_curw_adapter()
        adapter = None
        out_dir = tempfile.mkdtemp(prefix='metro_col_')
        extract_metro_colombo(res_mgr.get_resource_path('test/wrfout_d03_2017-10-02_12:00:00'), out_dir, out_dir,
                              curw_db_adapter=adapter, curw_db_upsert=True)

    def test_extract_mean_rainfall_from_shp_file(self):
        adapter = ext_utils.get_curw_adapter()
        basin_shp_file = res_mgr.get_resource_path('extraction/shp/kelani-upper-basin.shp')
        basin_extent = constants.KELANI_UPPER_BASIN_EXTENT
        extract_mean_rainfall_from_shp_file(res_mgr.get_resource_path('test/wrfout_d03_2017-10-02_12:00:00'),
                                            tempfile.mkdtemp(prefix='temp_'), 'kub_mean_rf', 'kub_mean', basin_shp_file,
                                            basin_extent, curw_db_adapter=adapter, curw_db_upsert=True)

    def test_create_rf_plots_wrf(self):
        out_base_dir = tempfile.mkdtemp(prefix='rf_plots_')
        out_dir = os.path.join(out_base_dir, 'plots_D03')
        create_rf_plots_wrf(res_mgr.get_resource_path('test/wrfout_d03_2017-12-09_18:00:00_rf'), out_dir, out_base_dir)

        out_dir = os.path.join(out_base_dir, 'plots_D01')
        lon_min, lat_min, lon_max, lat_max = constants.SRI_LANKA_D01_EXTENT
        create_rf_plots_wrf(res_mgr.get_resource_path('test/wrfout_d01_2017-12-09_18:00:00_rf'), out_dir, out_base_dir,
                            lat_min=lat_min, lon_min=lon_min, lat_max=lat_max, lon_max=lon_max)

    def test_kub_glencourse_flo2d_calibrate(self):
        out_base_dir = tempfile.mkdtemp(prefix='glencourse_')

        # rain = np.genfromtxt('/home/curw/Desktop/glen/rain.csv', delimiter=',', names=True, dtype=None,
        #                      converters={0: lambda s: dt.datetime.strptime(s.decode("utf-8"), '%Y-%m-%d %H:%M')})

        rain = np.genfromtxt('/home/curw/Desktop/glen/rain.csv', delimiter=',', names=True, dtype=None)

        coord = np.genfromtxt('/home/curw/Desktop/glen/coordinates.csv', names=True, delimiter=',', dtype=None)
        stations = {}
        for i, c in enumerate(coord):
            n = c[0].decode('utf-8')
            stations[n] = [c[2], c[1]]

        points = np.genfromtxt(
            '/home/curw/git/models/curw/rainfall/wrf/resources/extraction/local/klb_glecourse_points_150m.txt',
            delimiter=',', names=['id', 'lon', 'lat'], dtype=[int, float, float])

        thess_poly = spatial_utils.get_voronoi_polygons(stations,
                                                        '/home/curw/git/models/curw/rainfall/wrf/resources/extraction'
                                                        '/shp/klb_glencourse/klb_glencourse.shp',
                                                        add_total_area=False,
                                                        output_shape_file=os.path.join(out_base_dir, 'out.shp')
                                                        )

        region = [spatial_utils.is_inside_geo_df(thess_poly, points['lon'][i], points['lat'][i]) for i in
                  range(len(points))]

        with open(os.path.join(out_base_dir, 'raincell.dat'), 'w') as out:
            for r in rain:
                for i, p in enumerate(points):
                    out.write('%d %g\n' % (p[0], r[region[i]]))

    def test_extract_metro_col_rf_for_mike21(self):
        out_dir = tempfile.mkdtemp(prefix='met_col_mike21')
        prev = [res_mgr.get_resource_path('test/wrfout_d03_2017-12-10_18:00:00_rf'),
                res_mgr.get_resource_path('test/wrfout_d03_2017-12-09_18:00:00_rf')]
        extract_metro_col_rf_for_mike21(res_mgr.get_resource_path('test/wrfout_d03_2017-12-11_18:00:00_rf'), out_dir,
                                        prev_rf_files=prev)
