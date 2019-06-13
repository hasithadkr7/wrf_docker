import argparse
import json
import logging
import os
import shutil

from curw.container.docker.rainfall import utils as docker_rf_utils
from curw.rainfall.wrf import utils
from curw.rainfall.wrf.execution import executor


def parse_args():
    parser = argparse.ArgumentParser()
    env_vars = docker_rf_utils.get_env_vars('CURW_')

    def check_key(k, d_val):
        if k in env_vars and not env_vars[k]:
            return env_vars[k]
        else:
            return d_val

    parser.add_argument('-run_id', default=check_key('run_id', docker_rf_utils.id_generator()))
    parser.add_argument('-mode', default=check_key('mode', 'wps'))
    parser.add_argument('-nl_wps', default=check_key('nl_wps', None))
    parser.add_argument('-nl_input', default=check_key('nl_input', None))
    parser.add_argument('-wrf_config', default=check_key('wrf_config', '{}'))

    return parser.parse_args()


def run_wrf(wrf_config):
    logging.info('Running WRF')

    logging.info('Replacing the namelist input file')
    executor.replace_namelist_input(wrf_config)

    logging.info('Running WRF...')
    executor.run_em_real(wrf_config)


def run_wps(wrf_config):
    logging.info('Downloading GFS data')
    executor.download_gfs_data(wrf_config)

    logging.info('Replacing the namelist wps file')
    executor.replace_namelist_wps(wrf_config)

    logging.info('Running WPS...')
    executor.run_wps(wrf_config)

    logging.info('Cleaning up wps dir...')
    wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))
    shutil.rmtree(wrf_config.get('gfs_dir'))
    utils.delete_files_with_prefix(wps_dir, 'FILE:*')
    utils.delete_files_with_prefix(wps_dir, 'PFILE:*')
    utils.delete_files_with_prefix(wps_dir, 'geo_em.*')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    args = vars(parse_args())

    logging.info('Running arguments:\n%s' % json.dumps(args, sort_keys=True, indent=0))

    run_id = args['run_id']  # env_vars.pop('run_id', id_generator())
    logging.info('**** WRF RUN **** Run ID: ' + run_id)

    mode = args['mode'].strip().lower()  # env_vars.pop('mode').strip().lower()
    nl_wps = args['nl_wps']  # env_vars.pop('nl_wps', None)
    nl_input = args['nl_input']  # env_vars.pop('nl_input', None)

    logging.info('Getting wrf_config')
    wrf_config_dict = docker_rf_utils.get_config_dict_decoded(args['wrf_config'])
    config = executor.get_wrf_config(**wrf_config_dict)
    config.set('run_id', run_id)

    wrf_home = config.get('wrf_home')


    def write_wps():
        if nl_wps:
            logging.info('Reading namelist wps')
            nl_wps_path = os.path.join(wrf_home, 'namelist.wps')
            if os.path.isfile(nl_wps):
                logging.info('Using namelist wps path')
                shutil.copyfile(nl_wps, nl_wps_path)
            else:
                logging.info('Using namelist wps content')
                content = docker_rf_utils.get_base64_decoded_str(nl_wps)
                logging.debug('namelist.wps content: \n%s' % content)
                with open(nl_wps_path, 'w') as f:
                    f.write(content)
                    f.write('\n')
            config.set('namelist_wps', nl_wps_path)
        else:
            logging.info('Namelist wps not provided!')


    def write_input():
        if nl_input:
            logging.info('Reading namelist input')
            nl_input_path = os.path.join(wrf_home, 'namelist.input')
            if os.path.isfile(nl_input):
                logging.info('Using namelist input path')
                shutil.copyfile(nl_input, nl_input_path)
            else:
                logging.info('Using namelist input content')
                content = docker_rf_utils.get_base64_decoded_str(nl_input)
                logging.debug('namelist.input content: \n%s' % content)
                with open(nl_input_path, 'w') as f:
                    f.write(content)
                    f.write('\n')
            config.set('namelist_input', nl_input_path)
        else:
            logging.info('Namelist input not provided!')


    logging.info('WRF config: %s' % config.to_json_string())

    if mode == 'wps':
        logging.info('Running WPS')
        write_wps()
        run_wps(config)
    elif mode == 'wrf':
        logging.info('Running WRF')
        write_input()
        run_wrf(config)
    elif mode == "all":
        logging.info("Running both WPS and WRF")
        write_wps()
        write_input()
        run_wps(config)
        run_wrf(config)
    elif mode == "test":
        logging.info("Running on test mode: Nothing to do!")
        logging.info('namelist.wps content: \n%s' % docker_rf_utils.get_base64_decoded_str(nl_wps))
        logging.info('namelist.input content: \n%s' % docker_rf_utils.get_base64_decoded_str(nl_input))
    else:
        raise docker_rf_utils.CurwDockerRainfallException('Unknown mode ' + mode)
