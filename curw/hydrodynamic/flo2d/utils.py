import datetime as dt
import json
import unittest

import numpy as np

from curwmysqladapter import mysqladapter
from curw.rainfall.wrf.extraction import utils as rf_ext_utils
from curw.rainfall.wrf.resources import manager as res_mgr


def get_observed_rf(start, end, points, mysql_config_path=None):
    adapter = get_curw_adapter(mysql_config_path)
    opts = {
        'from': start.strftime('%Y-%m-%d %H:%M:%S'),
        'to': end.strftime('%Y-%m-%d %H:%M:%S')
    }

    metaQuery = {
        'station': points,
        'variable': 'Precipitation',
        'type': 'Observed'
    }

    result = adapter.retrieveTimeseries(metaQuery, opts)
    pass


def get_curw_adapter(mysql_config_path=None):
    if mysql_config_path is None:
        mysql_config_path = res_mgr.get_resource_path('config/mysql_config.json')

    with open(mysql_config_path) as data_file:
        config = json.load(data_file)

    return mysqladapter(host=config['host'], user=config['user'], password=config['password'], db=config['db'])


def create_raincell_from_wrf(run_ts, wrf_out, raincell_points_file, observation_points, output, mysql_config_path=None):
    """
    
    :param run_ts: running timestamp %Y:%m:%d_%H:%M:%S 
    :param wrf_out: corresponding wrf output location 
    :param raincell_points_file: file with the flo2d raincell points - [id, lat, lon]
    :param observation_points: observation points array of array [[id, lat, lon]]
    :param output: output file location 
    """
    raincell_points = np.genfromtxt(raincell_points_file, delimiter=',')

    lon_min = np.min(raincell_points, 0)[1]
    lat_min = np.min(raincell_points, 0)[2]
    lon_max = np.max(raincell_points, 0)[1]
    lat_max = np.max(raincell_points, 0)[2]

    rf_vars = ['RAINC', 'RAINNC']

    rf_values = rf_ext_utils.extract_variables(wrf_out, rf_vars, lat_min, lat_max, lon_min, lon_max)

    cum_precip = rf_values[rf_vars[0]]
    for i in range(1, len(rf_vars)):
        cum_precip = cum_precip + rf_values[rf_vars[i]]

    ts_idx = int(np.argwhere(rf_values['Times'] == run_ts))

    observed = get_observed_rf(start, end, points, mysql_config_path)

    pass


def suite():
    s = unittest.TestSuite()
    s.addTest(TestFlo2dUtils)
    return s


class TestFlo2dUtils(unittest.TestCase):
    # def test_get_weather_type(self):
    #     logging.basicConfig(level=logging.INFO,
    #                         format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    #
    #     create_raincell_from_wrf('2017-08-22_06:00:00',
    #                              '/home/curw/wrf_compare/mnt/disks/curwsl_nfs/output/wrf0/2017-08-22_00:00/0/'
    #                              'wrfout_d03_2017-08-22_00:00:00_SL',
    #                              res_mgr.get_resource_path('extraction/local/kelani_basin_points_250m.txt'),
    #                              res_mgr.get_resource_path('extraction/shp/kelani-upper-basin.shp'),
    #                              '/tmp/raincell'
    #                              )

    def test_get_observed_rf(self):
        start = dt.datetime.strptime('2017-09-12 00:00:00', '%Y-%m-%d %H:%M:%S')
        end = dt.datetime.strptime('2017-09-13 00:00:00', '%Y-%m-%d %H:%M:%S')

        points = ['Kompannaveediya', 'Borella']

        get_observed_rf(start, end, points)
