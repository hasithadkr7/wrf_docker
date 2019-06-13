import argparse
import ast
import datetime as dt
import glob
import json
import logging
import os
import shutil
import traceback
from tempfile import TemporaryDirectory

import sys

from curw.container.docker.rainfall import utils as docker_rf_utils
from curw.rainfall.wrf import utils
from curw.rainfall.wrf.execution.executor import get_wrf_config
from curw.rainfall.wrf.extraction import extractor, constants
from curw.rainfall.wrf.extraction import utils as ext_utils
from curw.rainfall.wrf.resources import manager as res_mgr
from curwmysqladapter import Data


def parse_args():
    parser = argparse.ArgumentParser()
    env_vars = docker_rf_utils.get_env_vars('CURW_')

    def check_key(k, d_val):
        if k in env_vars and not env_vars[k]:
            return env_vars[k]
        else:
            return d_val

    parser.add_argument('-run_id', default=check_key('run_id', docker_rf_utils.id_generator()))
    parser.add_argument('-db_config', default=check_key('db_config', docker_rf_utils.get_base64_encoded_str('{}')))
    parser.add_argument('-wrf_config', default=check_key('wrf_config', docker_rf_utils.get_base64_encoded_str('{}')))
    parser.add_argument('-overwrite', default=check_key('overwrite', 'False'))
    parser.add_argument('-data_type', default=check_key('data_type', 'data'))
    parser.add_argument('-procedures', default=check_key('procedures', str(sys.maxsize)))

    return parser.parse_args()


def run(run_id, wrf_config_dict, db_config_dict, upsert=False, run_name='Cloud-1', data_type=Data.data,
        procedures=sys.maxsize):
    """
    Procedure #1: Extracting data from D03
    Procedure #2: Extract rainfall data for the metro colombo area
    Procedure #3: Extract weather station rainfall
    Procedure #4: Extract Kelani upper Basin mean rainfall
    Procedure #5: Extract Kelani lower Basin mean rainfall
    Procedure #6: Create plots for D03
    Procedure #7: Extract Kelani lower Basin rainfall for FLO2D
    Procedure #8: Extract Kelani lower Basin rainfall for MIKE21
    Procedure #9: Create plots for D01
    Procedure #10: Extract rf data from metro col for MIKE21
    """

    def _nth_bit(a, n):
        return (a >> n) & 1

    logging.info('**** Extracting data from WRF **** Run ID: ' + run_id)
    run_prefix = run_id.split('_')[0]

    config = get_wrf_config(**wrf_config_dict)
    config.set('run_id', run_id)

    output_dir_base = os.path.join(config.get('nfs_dir'), 'results')
    run_output_dir = os.path.join(output_dir_base, run_id)
    wrf_output_dir = os.path.join(run_output_dir, 'wrf')
    logging.info('WRF output dir: ' + wrf_output_dir)

    db_adapter = ext_utils.get_curw_adapter(mysql_config=db_config_dict) if db_config_dict else None

    logging.info('Creating temp file space')

    with TemporaryDirectory(prefix='wrfout_') as temp_dir:
        try:
            logging.info('Copying wrfout_D03* to temp_dir ' + temp_dir)
            d03_nc_f = shutil.copy2(glob.glob(os.path.join(wrf_output_dir, 'wrfout_d03_*'))[0], temp_dir)

            if _nth_bit(procedures, 1):
                logging.info('Procedure #1: Extracting data from ' + d03_nc_f)
                try:
                    logging.info('Extract WRF data points in the Kelani and Kalu basins')
                    lon_min, lat_min, lon_max, lat_max = constants.KELANI_KALU_BASIN_EXTENT
                    extractor.push_wrf_rainfall_to_db(d03_nc_f, curw_db_adapter=db_adapter, lat_min=lat_min,
                                                      lon_min=lon_min, lat_max=lat_max, lon_max=lon_max,
                                                      run_prefix=run_prefix, upsert=upsert)
                except Exception as e:
                    logging.error('Extract WRF data points in the Kelani and Kalu basins FAILED: ' + str(
                        e) + '\n' + traceback.format_exc())

            if _nth_bit(procedures, 2):
                try:
                    logging.info('Procedure #2: Extract rainfall data for the metro colombo area')
                    basin_rf = extractor.extract_metro_colombo(d03_nc_f, run_output_dir, output_dir_base,
                                                               curw_db_adapter=db_adapter, run_prefix=run_prefix,
                                                               run_name=run_name, curw_db_upsert=upsert)
                    logging.info('Basin rainfall' + str(basin_rf))
                except Exception as e:
                    logging.error('Extract rainfall data for the metro colombo area FAILED: ' + str(
                        e) + '\n' + traceback.format_exc())

            if _nth_bit(procedures, 3):
                try:
                    logging.info('Procedure #3: Extract weather station rainfall')
                    extractor.extract_weather_stations(d03_nc_f, run_output_dir, curw_db_adapter=db_adapter,
                                                       curw_db_upsert=upsert, run_prefix=run_prefix, run_name=run_name)
                except Exception as e:
                    logging.error('Extract weather station rainfall FAILED: ' + str(e) + '\n' + traceback.format_exc())

            if _nth_bit(procedures, 4):
                try:
                    logging.info('Procedure #4: Extract Kelani upper Basin mean rainfall')
                    basin_shp_file = res_mgr.get_resource_path('extraction/shp/kelani-upper-basin.shp')
                    extractor.extract_mean_rainfall_from_shp_file(d03_nc_f, run_output_dir, 'kub_mean_rf', 'kub_mean',
                                                                  basin_shp_file, constants.KELANI_UPPER_BASIN_EXTENT,
                                                                  curw_db_adapter=db_adapter, run_prefix=run_prefix,
                                                                  run_name=run_name, curw_db_upsert=upsert)
                except Exception as e:
                    logging.error(
                        'Extract Kelani upper Basin mean rainfall FAILED: ' + str(e) + '\n' + traceback.format_exc())

            if _nth_bit(procedures, 5):
                try:
                    logging.info('Procedure #5: Extract Kelani lower Basin mean rainfall')
                    basin_shp_file = res_mgr.get_resource_path('extraction/shp/klb-wgs84/klb-wgs84.shp')
                    extractor.extract_mean_rainfall_from_shp_file(d03_nc_f, run_output_dir, 'klb_mean_rf', 'klb_mean',
                                                                  basin_shp_file, constants.KELANI_LOWER_BASIN_EXTENT,
                                                                  curw_db_adapter=db_adapter, run_prefix=run_prefix,
                                                                  run_name=run_name, curw_db_upsert=upsert)
                except Exception as e:
                    logging.error(
                        'Extract Kelani lower Basin mean rainfall FAILED: ' + str(e) + '\n' + traceback.format_exc())

            if _nth_bit(procedures, 6):
                try:
                    logging.info('Procedure #6: Create plots for D03')
                    lon_min, lat_min, lon_max, lat_max = constants.SRI_LANKA_EXTENT
                    extractor.create_rf_plots_wrf(d03_nc_f, os.path.join(run_output_dir, 'plots_D03'), output_dir_base,
                                                  lat_min=lat_min, lon_min=lon_min, lat_max=lat_max, lon_max=lon_max,
                                                  run_prefix=run_prefix)
                except Exception as e:
                    logging.error('Create plots for D03 FAILED: ' + str(e) + '\n' + traceback.format_exc())

            if _nth_bit(procedures, 7):
                try:
                    logging.info('Procedure #7: Extract Kelani lower Basin rainfall for FLO2D')
                    run_date = dt.datetime.strptime(config.get('start_date'), '%Y-%m-%d_%H:%M')

                    prev_days = 5
                    d03_nc_f_prev = []
                    for i in range(prev_days):
                        prev = '_'.join(
                            [run_prefix, (run_date - dt.timedelta(days=i + 1)).strftime('%Y-%m-%d_%H:%M'), '*'])
                        try:
                            d03_nc_f_prev.append(shutil.copy2(
                                glob.glob(os.path.join(output_dir_base, prev, 'wrf', 'wrfout_d03_*'))[0], temp_dir))
                        except IndexError as e:
                            logging.warning('File for %s not found. Filling with 0s: %s' % (prev, str(e)))
                            d03_nc_f_prev.append(None)

                    logging.info('250m model')
                    kelani_basin_flo2d_file = res_mgr.get_resource_path('extraction/local/kelani_basin_points_250m.txt')
                    extractor.extract_kelani_basin_rainfall_flo2d(d03_nc_f, d03_nc_f_prev[:2],
                                                                  os.path.join(run_output_dir, 'klb_flo2d'),
                                                                  kelani_basin_file=kelani_basin_flo2d_file)
                    logging.info('150m model')
                    kelani_basin_flo2d_file = res_mgr.get_resource_path(
                        'extraction/local/klb_glecourse_points_150m.txt')
                    extractor.extract_kelani_basin_rainfall_flo2d(d03_nc_f, d03_nc_f_prev,
                                                                  os.path.join(run_output_dir, 'klb_flo2d'),
                                                                  kelani_basin_file=kelani_basin_flo2d_file,
                                                                  output_prefix='RAINCELL_150m', target_rfs=[])

                except Exception as e:
                    logging.error('Extract Kelani lower Basin mean rainfall for FLO2D FAILED: ' + str(
                        e) + '\n' + traceback.format_exc())

            if _nth_bit(procedures, 8):
                try:
                    logging.info('Procedure #8: Extract Kelani lower Basin rainfall for MIKE21')
                    run_date = dt.datetime.strptime(config.get('start_date'), '%Y-%m-%d_%H:%M')
                    now = shutil.copy2(
                        os.path.join(output_dir_base, config.get('run_id'), 'klb_mean_rf', 'klb_mean_rf.txt'),
                        os.path.join(temp_dir, 'klb.txt'))
                    prev_1 = '_'.join([run_prefix, (run_date - dt.timedelta(days=1)).strftime('%Y-%m-%d_%H:%M'), '*'])
                    prev_2 = '_'.join([run_prefix, (run_date - dt.timedelta(days=2)).strftime('%Y-%m-%d_%H:%M'), '*'])

                    try:
                        klb_prev_1 = utils.copy_if_not_exists(
                            glob.glob(os.path.join(output_dir_base, prev_1, 'klb_mean_rf', 'klb_mean_rf.txt'))[0],
                            os.path.join(temp_dir, 'klb1.txt'))
                    except IndexError as e:
                        logging.warning('File for %s not found. Filling with 0s: %s' % (prev_1, str(e)))
                        klb_prev_1 = None

                    try:
                        klb_prev_2 = utils.copy_if_not_exists(
                            glob.glob(os.path.join(output_dir_base, prev_2, 'klb_mean_rf', 'klb_mean_rf.txt'))[0],
                            os.path.join(temp_dir, 'klb2.txt'))
                    except IndexError as e:
                        logging.warning('File for %s not found. Filling with 0s: %s' % (prev_2, str(e)))
                        klb_prev_2 = None

                    extractor.create_rainfall_for_mike21(now, [klb_prev_1, klb_prev_2],
                                                         os.path.join(run_output_dir, 'klb_mike21'))
                except Exception as e:
                    logging.error('Extract Kelani lower Basin mean rainfall for MIKE21 FAILED: ' + str(
                        e) + '\n' + traceback.format_exc())

            if _nth_bit(procedures, 10):
                try:
                    logging.info('Procedure #10: Extract rf data from metro col for MIKE21')
                    run_date = dt.datetime.strptime(config.get('start_date'), '%Y-%m-%d_%H:%M')
                    prev_1 = '_'.join([run_prefix, (run_date - dt.timedelta(days=1)).strftime('%Y-%m-%d_%H:%M'), '*'])
                    prev_2 = '_'.join([run_prefix, (run_date - dt.timedelta(days=2)).strftime('%Y-%m-%d_%H:%M'), '*'])

                    try:
                        d03_nc_f_prev_1 = utils.copy_if_not_exists(
                            glob.glob(os.path.join(output_dir_base, prev_1, 'wrf', 'wrfout_d03_*'))[0], temp_dir)
                    except IndexError as e:
                        logging.warning('File for %s not found. Filling with 0s: %s' % (prev_1, str(e)))
                        d03_nc_f_prev_1 = None

                    try:
                        d03_nc_f_prev_2 = utils.copy_if_not_exists(
                            glob.glob(os.path.join(output_dir_base, prev_2, 'wrf', 'wrfout_d03_*'))[0], temp_dir)
                    except IndexError as e:
                        logging.warning('File for %s not found. Filling with 0s: %s' % (prev_2, str(e)))
                        d03_nc_f_prev_2 = None

                    prev = [d03_nc_f_prev_1, d03_nc_f_prev_2]

                    extractor.extract_metro_col_rf_for_mike21(d03_nc_f, os.path.join(run_output_dir, 'met_col_mike21'),
                                                              prev_rf_files=prev)
                except Exception as e:
                    logging.error('Extract rf data from metro col for MIKE21: ' + str(
                        e) + '\n' + traceback.format_exc())

        except Exception as e:
            logging.error(
                'Copying wrfout_d03_* to temp_dir %s FAILED: %s\n%s' % (temp_dir, str(e), traceback.format_exc()))

        try:
            d01_nc_f = shutil.copy2(glob.glob(os.path.join(wrf_output_dir, 'wrfout_d01_*'))[0], temp_dir)

            logging.info('Extracting data from ' + d01_nc_f)

            if _nth_bit(procedures, 9):
                try:
                    logging.info('Procedure #9: Create plots for D01')
                    lon_min, lat_min, lon_max, lat_max = constants.SRI_LANKA_D01_EXTENT
                    extractor.create_rf_plots_wrf(d01_nc_f, os.path.join(run_output_dir, 'plots_D01'), output_dir_base,
                                                  lat_min=lat_min, lon_min=lon_min, lat_max=lat_max, lon_max=lon_max,
                                                  run_prefix=run_prefix)
                except Exception as e:
                    logging.error('Create plots for D01 FAILED: ' + str(e) + '\n' + traceback.format_exc())
        except Exception as e:
            logging.error(
                'Copying wrfout_d01_* to temp_dir %s FAILED: %s\n%s' % (temp_dir, str(e), traceback.format_exc()))

    logging.info('**** Extracting data from WRF **** Run ID: ' + run_id + ' COMPLETED!')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    logging.info('Received arguments:\n%s' % sys.argv[1:])

    args = vars(parse_args())

    logging.info('Running arguments:\n%s' % json.dumps(args, sort_keys=True, indent=0))

    logging.info('Getting wrf_config')
    wrf_config = docker_rf_utils.get_config_dict_decoded(args['wrf_config'])
    logging.debug('Wrf config:\n' + str(wrf_config))

    logging.info('Getting db_config')
    db_config = docker_rf_utils.get_config_dict_decoded(args['db_config'])
    logging.debug('Db config:\n' + str(db_config))

    procedures_tag = int(args['procedures'], base=0)
    logging.info('Following procedures will be extracted: %s' % (
        'all' if procedures_tag == sys.maxsize else "{0:b}".format(procedures_tag)))

    run(args['run_id'], wrf_config, db_config, ast.literal_eval(args['overwrite']), procedures=procedures_tag,
        data_type=ext_utils.parse_database_data_type(args['data_type']))
