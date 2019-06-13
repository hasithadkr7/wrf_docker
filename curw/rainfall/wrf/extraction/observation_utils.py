import datetime as dt
import glob
import logging
import os
import shutil
import tempfile
import unittest

import numpy as np
import pandas as pd
from curw.rainfall.wrf import utils
from curw.rainfall.wrf.extraction import spatial_utils
from curw.rainfall.wrf.extraction import utils as ext_utils
from curw.rainfall.wrf.resources import manager as res_mgr


class CurwObservationException(Exception):
    pass


def extract_kelani_basin_rainfall_flo2d_with_obs(nc_f, adapter, obs_stations, output_dir, start_ts_lk,
                                                 duration_days=None, output_prefix='RAINCELL',
                                                 kelani_lower_basin_points=None, kelani_lower_basin_shp=None):
    """
    check test_extract_kelani_basin_rainfall_flo2d_obs test case
    :param nc_f: file path of the wrf output
    :param adapter:
    :param obs_stations: dict of stations. {station_name: [lon, lat, name variable, nearest wrf point station name]}
    :param output_dir:
    :param start_ts_lk: start time of the forecast/ end time of the observations
    :param duration_days: (optional) a tuple (observation days, forecast days) default (2,3)
    :param output_prefix: (optional) output file name of the RAINCELL file. ex: output_prefix=RAINCELL-150m --> RAINCELL-150m.DAT
    :param kelani_lower_basin_points: (optional)
    :param kelani_lower_basin_shp: (optional)
    :return:
    """
    if duration_days is None:
        duration_days = (2, 3)

    if kelani_lower_basin_points is None:
        kelani_lower_basin_points = res_mgr.get_resource_path('extraction/local/kelani_basin_points_250m.txt')

    if kelani_lower_basin_shp is None:
        kelani_lower_basin_shp = res_mgr.get_resource_path('extraction/shp/klb-wgs84/klb-wgs84.shp')

    points = np.genfromtxt(kelani_lower_basin_points, delimiter=',')

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

    utils.create_dir_if_not_exists(output_dir)

    obs_start = dt.datetime.strptime(start_ts_lk, '%Y-%m-%d_%H:%M') - dt.timedelta(days=duration_days[0])
    obs_end = dt.datetime.strptime(start_ts_lk, '%Y-%m-%d_%H:%M')
    forecast_end = dt.datetime.strptime(start_ts_lk, '%Y-%m-%d_%H:%M') + dt.timedelta(days=duration_days[1])

    obs = _get_observed_precip(obs_stations, obs_start, obs_end, duration_days, adapter)

    thess_poly = spatial_utils.get_voronoi_polygons(obs_stations, kelani_lower_basin_shp, add_total_area=False)

    output_file_path = os.path.join(output_dir, output_prefix + '.DAT')

    # update points array with the thessian polygon idx
    point_thess_idx = []
    for point in points:
        point_thess_idx.append(spatial_utils.is_inside_geo_df(thess_poly, lon=point[1], lat=point[2]))
        pass

    with open(output_file_path, 'w') as output_file:
        res_mins = int((t1 - t0).total_seconds() / 60)
        data_hours = int(sum(duration_days) * 24 * 60 / res_mins)
        start_ts_lk = obs_start.strftime('%Y-%m-%d %H:%M:%S')
        end_ts = forecast_end.strftime('%Y-%m-%d %H:%M:%S')

        output_file.write("%d %d %s %s\n" % (res_mins, data_hours, start_ts_lk, end_ts))

        for t in range(int(24 * 60 * duration_days[0] / res_mins) + 1):
            for i, point in enumerate(points):
                rf = float(obs[point_thess_idx[i]].values[t]) if point_thess_idx[i] is not None else 0
                output_file.write('%d %.1f\n' % (point[0], rf))

        forecast_start_idx = int(
            np.where(times == utils.datetime_lk_to_utc(obs_end, shift_mins=30).strftime('%Y-%m-%d_%H:%M:%S'))[0])
        for t in range(int(24 * 60 * duration_days[1] / res_mins) - 1):
            for point in points:
                rf_x = np.digitize(point[1], lon_bins)
                rf_y = np.digitize(point[2], lat_bins)
                if t + forecast_start_idx + 1 < len(times):
                    output_file.write('%d %.1f\n' % (point[0], diff[t + forecast_start_idx + 1, rf_y, rf_x]))
                else:
                    output_file.write('%d %.1f\n' % (point[0], 0))


def create_rainfall_for_mike21_obs(d0_rf_file, adapter, obs_stations, output_dir, start_ts, duration_days=None,
                                   kelani_lower_basin_shp=None):
    if kelani_lower_basin_shp is None:
        kelani_lower_basin_shp = res_mgr.get_resource_path('extraction/shp/klb-wgs84/klb-wgs84.shp')

    if duration_days is None:
        duration_days = (2, 3)

    obs_start = dt.datetime.strptime(start_ts, '%Y-%m-%d_%H:%M') - dt.timedelta(days=duration_days[0])
    obs_end = dt.datetime.strptime(start_ts, '%Y-%m-%d_%H:%M')
    # forecast_end = dt.datetime.strptime(start_ts, '%Y-%m-%d_%H:%M') + dt.timedelta(days=duration_days[1])

    obs = _get_observed_precip(obs_stations, obs_start, obs_end, duration_days, adapter)

    thess_poly = spatial_utils.get_voronoi_polygons(obs_stations, kelani_lower_basin_shp, add_total_area=False)

    observed = None
    for i, _id in enumerate(thess_poly['id']):
        if observed is not None:
            observed = observed + obs[_id].astype(float) * thess_poly['area'][i]
        else:
            observed = obs[_id].astype(float) * thess_poly['area'][i]
    observed = observed / sum(thess_poly['area'])

    d0 = np.genfromtxt(d0_rf_file, dtype=str)
    t0 = dt.datetime.strptime(' '.join(d0[0][:-1]), '%Y-%m-%d %H:%M:%S')
    t1 = dt.datetime.strptime(' '.join(d0[1][:-1]), '%Y-%m-%d %H:%M:%S')

    res_min = int((t1 - t0).total_seconds() / 60)

    # prev_output = np.append(prev_output, d0, axis=0)
    out_file = os.path.join(utils.create_dir_if_not_exists(output_dir), 'rf_mike21_obs.txt')

    with open(out_file, 'w') as out_f:
        for index in observed.index:
            out_f.write('%s:00\t%.4f\n' % (index, observed.precip[index]))

        forecast_start_idx = int(
            np.where((d0[:, 0] == obs_end.strftime('%Y-%m-%d')) & (d0[:, 1] == obs_end.strftime('%H:%M:%S')))[0])
        # note: no need to convert to utc as rf_mike21.txt has times in LK

        for i in range(forecast_start_idx + 1, int(24 * 60 * duration_days[1] / res_min)):
            if i < len(d0):
                out_f.write('%s %s\t%s\n' % (d0[i][0], d0[i][1], d0[i][2]))
            else:
                out_f.write('%s\t0.0\n' % (obs_end + dt.timedelta(hours=i - forecast_start_idx - 1)).strftime(
                    '%Y-%m-%d %H:%M:%S'))


def _get_observed_precip(obs_stations, start_dt, end_dt, duration_days, adapter, forecast_source='wrf0', ):
    def _validate_ts(_s, _ts_sum, _opts):
        if len(_ts_sum) == duration_days[0] * 24 + 1:
            return

        logging.warning('%s Validation count fails. Trying to fill forecast for missing values' % _s)
        f_station = {'station': obs_stations[_s][3],
                     'variable': 'Precipitation',
                     'unit': 'mm',
                     'type': 'Forecast-0-d',
                     'source': forecast_source,
                     }
        f_ts = np.array(adapter.retrieve_timeseries(f_station, _opts)[0]['timeseries'])

        if len(f_ts) != duration_days[0] * 24 + 1:
            raise CurwObservationException('%s Forecast time-series validation failed' % _s)

        for j in range(duration_days[0] * 24 + 1):
            d = start_dt + dt.timedelta(hours=j)
            d_str = d.strftime('%Y-%m-%d %H:00')
            if j < len(_ts_sum.index.values):
                if _ts_sum.index[j] != d_str:
                    _ts_sum.loc[d_str] = f_ts[j, 1]
                    _ts_sum.sort_index(inplace=True)
            else:
                _ts_sum.loc[d_str] = f_ts[j, 1]

        if len(_ts_sum) == duration_days[0] * 24 + 1:
            return
        else:
            raise CurwObservationException('time series validation failed')

    obs = {}
    opts = {
        'from': start_dt.strftime('%Y-%m-%d %H:%M:%S'),
        'to': end_dt.strftime('%Y-%m-%d %H:%M:%S'),
    }

    for s in obs_stations.keys():
        station = {'station': s,
                   'variable': 'Precipitation',
                   'unit': 'mm',
                   'type': 'Observed',
                   'source': 'WeatherStation',
                   'name': obs_stations[s][2]
                   }

        ts = np.array(adapter.retrieve_timeseries(station, opts)[0]['timeseries'])
        ts_df = pd.DataFrame(data=ts, columns=['ts', 'precip'], index=ts[0:])
        ts_sum = ts_df.groupby(by=[ts_df.ts.map(lambda x: x.strftime('%Y-%m-%d %H:00'))]).sum()

        _validate_ts(s, ts_sum, opts)

        obs[s] = ts_sum

    return obs


def suite():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    s = unittest.TestSuite()
    s.addTest(TestExtractor)
    return s


class TestExtractor(unittest.TestCase):
    def test_extract_kelani_basin_rainfall_flo2d_obs(self):
        mysql_conf_path = '/home/curw/Desktop/2018-05/mysql.json'
        adapter = ext_utils.get_curw_adapter(mysql_config_path=mysql_conf_path)
        wrf_output_dir = tempfile.mkdtemp(prefix='flo2d_obs_')
        files = ['wrfout_d03_2018-05-23_18:00:00_rf']
        run_prefix = 'wrf0'

        for f in files:
            out_dir = utils.create_dir_if_not_exists(
                os.path.join(wrf_output_dir, f.replace('wrfout_d03', run_prefix).replace(':00_rf', '_0000'), 'wrf'))
            shutil.copy2('/home/curw/Desktop/2018-05/2018-05-23_18:00/wrf0/%s' % f, out_dir)

        run_date = dt.datetime.strptime('2018-05-23_18:00', '%Y-%m-%d_%H:%M')
        start_ts_lk = '2018-05-26_00:00'
        now = '_'.join([run_prefix, run_date.strftime('%Y-%m-%d_%H:%M'), '*'])

        d03_nc_f = glob.glob(os.path.join(wrf_output_dir, now, 'wrf', 'wrfout_d03_*'))[0]

        obs_stations = {
            'Kottawa North Dharmapala School': [79.95818, 6.865576, 'A&T Labs', 'wrf_79.957123_6.859688'],
            'IBATTARA2': [79.919, 6.908, 'CUrW IoT', 'wrf_79.902664_6.913757'],
            'Malabe': [79.95738, 6.90396, 'A&T Labs', 'wrf_79.957123_6.913757'],
            # 'Mutwal': [79.8609, 6.95871, 'A&T Labs', 'wrf_79.875435_6.967812'],
            # 'Mulleriyawa': [79.941176, 6.923571, 'A&T Labs', 'wrf_79.929893_6.913757'],
            'Orugodawatta': [79.87887, 6.943741, 'CUrW IoT', 'wrf_79.875435_6.940788'],
        }

        duration_days = (8, 0)

        # kelani_lower_basin_points = res_mgr.get_resource_path('extraction/local/kelani_basin_points_30m.txt')
        kelani_lower_basin_points = None

        extract_kelani_basin_rainfall_flo2d_with_obs(d03_nc_f, adapter, obs_stations,
                                                     os.path.join(wrf_output_dir, now, 'klb_flo2d'), start_ts_lk,
                                                     kelani_lower_basin_points=kelani_lower_basin_points,
                                                     duration_days=duration_days)

    def test_extract_kelani_basin_rainfall_flo2d_obs_150m(self):
        adapter = ext_utils.get_curw_adapter(mysql_config_path='/home/curw/Desktop/2018-05/mysql.json')
        wrf_output_dir = tempfile.mkdtemp(prefix='flo2d_obs_')
        files = ['wrfout_d03_2018-05-23_18:00:00_rf']
        run_prefix = 'wrf0'

        for f in files:
            out_dir = utils.create_dir_if_not_exists(
                os.path.join(wrf_output_dir, f.replace('wrfout_d03', run_prefix).replace(':00_rf', '_0000'), 'wrf'))
            shutil.copy2('/home/curw/Desktop/2018-05/2018-05-23_18:00/wrf0/%s' % f, out_dir)

        run_date = dt.datetime.strptime('2018-05-23_18:00', '%Y-%m-%d_%H:%M')
        now = '_'.join([run_prefix, run_date.strftime('%Y-%m-%d_%H:%M'), '*'])

        d03_nc_f = glob.glob(os.path.join(wrf_output_dir, now, 'wrf', 'wrfout_d03_*'))[0]

        obs_stations = {'Kottawa North Dharmapala School': [79.95818, 6.865576, 'A&T Labs', 'wrf_79.957123_6.859688'],
                        'IBATTARA2': [79.919, 6.908, 'CUrW IoT', 'wrf_79.902664_6.913757'],
                        'Malabe': [79.95738, 6.90396, 'A&T Labs', 'wrf_79.957123_6.913757'],
                        # 'Mutwal': [79.8609, 6.95871, 'A&T Labs', 'wrf_79.875435_6.967812'],
                        'Glencourse': [80.20305, 6.97805, 'Irrigation Department', 'wrf_80.202187_6.967812'],
                        # 'Waga': [80.11828, 6.90678, 'A&T Labs', 'wrf_80.120499_6.913757'],
                        }

        start_ts = '2018-05-26_00:00'
        kelani_lower_basin_points = res_mgr.get_resource_path('extraction/local/klb_glecourse_points_150m.txt')
        kelani_lower_basin_shp = res_mgr.get_resource_path('extraction/shp/klb_glencourse/klb_glencourse.shp')
        duration_days = (8, 0)
        extract_kelani_basin_rainfall_flo2d_with_obs(d03_nc_f, adapter, obs_stations,
                                                     os.path.join(wrf_output_dir, now, 'klb_flo2d'), start_ts,
                                                     duration_days=duration_days,
                                                     kelani_lower_basin_shp=kelani_lower_basin_shp,
                                                     kelani_lower_basin_points=kelani_lower_basin_points,
                                                     output_prefix='RAINCELL_150m')

    def test_create_rainfall_for_mike21_obs(self):
        adapter = ext_utils.get_curw_adapter()
        wrf_output_dir = tempfile.mkdtemp(prefix='mike21_obs_')

        out_dir = utils.create_dir_if_not_exists(os.path.join(wrf_output_dir, 'klb_mean_rf'))
        shutil.copy2('/home/curw/Desktop/2018-05/klb_mean_rf/klb_mean_rf.txt', out_dir)

        d0_mean_rf = os.path.join(out_dir, 'klb_mean_rf.txt')

        obs_stations = {'Kottawa North Dharmapala School': [79.95818, 6.865576, 'A&T Labs'],
                        'IBATTARA2': [79.919, 6.908, 'CUrW IoT'],
                        'Malabe': [79.95738, 6.90396, 'A&T Labs'],
                        'Mutwal': [79.8609, 6.95871, 'A&T Labs']}

        start_ts = '2018-05-21_00:00'
        create_rainfall_for_mike21_obs(d0_mean_rf, adapter, obs_stations, out_dir, start_ts, )
