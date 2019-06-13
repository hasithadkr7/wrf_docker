#!/usr/bin/python
import json
import logging
import os

from airflow.models import Variable
from curw.rainfall.wrf.execution import executor
from curw.rainfall.wrf.execution.executor import WrfConfig
from curw.rainfall.wrf import utils


def preprocess(**kwargs):
    # todo: these small steps can also be parallized
    logging.info('Running preprocessing for WPS...')
    wrf_config = get_wrf_config(**kwargs)

    logging.info('Replacing namellist.wps place-holders')
    executor.replace_namelist_wps(wrf_config)


def pre_ungrib(**kwargs):
    logging.info('Running preprocessing for ungrib...')

    wrf_config = get_wrf_config(**kwargs)

    wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))
    logging.info('WPS dir: %s' % wps_dir)

    logging.info('Cleaning up files')
    utils.delete_files_with_prefix(wps_dir, 'FILE:*')
    utils.delete_files_with_prefix(wps_dir, 'PFILE:*')

    # Linking VTable
    if not os.path.exists(os.path.join(wps_dir, 'Vtable')):
        logging.info('Creating Vtable symlink')
        os.symlink(os.path.join(wps_dir, 'ungrib/Variable_Tables/Vtable.NAM'), os.path.join(wps_dir, 'Vtable'))
    pass


def ungrib(**kwargs):
    logging.info('Running ungrib...')

    wrf_config = get_wrf_config(**kwargs)
    wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))

    # Running link_grib.csh
    logging.info('Running link_grib.csh')
    gfs_date, gfs_cycle, start = utils.get_appropriate_gfs_inventory(wrf_config)
    dest = \
        utils.get_gfs_data_url_dest_tuple(wrf_config.get('gfs_url'), wrf_config.get('gfs_inv'), gfs_date, gfs_cycle, '',
                                          wrf_config.get('gfs_res'), '')[1]  # use get_gfs_data_url_dest_tuple to get
    utils.run_subprocess(
        'csh link_grib.csh %s/%s' % (wrf_config.get('gfs_dir'), dest), cwd=wps_dir)

    utils.run_subprocess('./ungrib.exe', cwd=wps_dir)


def pre_metgrid(**kwargs):
    logging.info('Running preporcessing for geogrid...')

    wrf_config = get_wrf_config(**kwargs)
    wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))
    utils.delete_files_with_prefix(wps_dir, 'met_em*')


def metgrid(**kwargs):
    logging.info('Running metgrid...')

    wrf_config = get_wrf_config(**kwargs)
    wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))

    utils.run_subprocess('./metgrid.exe', cwd=wps_dir)


def pre_geogrid(**kwargs):
    logging.info('Running preprocessing for geogrid...')


def geogrid(**kwargs):
    logging.info('Running geogrid...')

    wrf_config = get_wrf_config(**kwargs)
    wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))

    if not executor.check_geogrid_output(wps_dir):
        logging.info('Running Geogrid.exe')
        utils.run_subprocess('./geogrid.exe', cwd=wps_dir)
    else:
        logging.info('Geogrid output already available')


def get_wrf_config(**kwargs):
    if 'ti' in kwargs:
        wrf_config_json = kwargs['ti'].xcom_pull(task_ids=None, key='wrf_config_json')
        logging.info('wrf_config from xcom: ' + wrf_config_json)
        wrf_config = WrfConfig(json.loads(wrf_config_json))
    else:
        try:
            wrf_config = WrfConfig(Variable.get('wrf_config', deserialize_json=True))
        except KeyError:
            raise RunUngribTaskException('Unable to find WrfConfig')
    return wrf_config


class RunUngribTaskException(Exception):
    def __init__(self, msg):
        self.msg = msg
        Exception.__init__(self, msg)
