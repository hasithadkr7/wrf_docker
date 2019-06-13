import datetime as dt
import glob
import json
import logging
import os
import tempfile
import threading
import time
import unittest
from urllib.error import HTTPError, URLError
from zipfile import ZipFile, ZIP_DEFLATED

from curw.rainfall.wrf.resources import manager as res_mgr
from curw.rainfall.wrf import constants, utils


def download_single_inventory(url, dest, retries=constants.DEFAULT_RETRIES, delay=constants.DEFAULT_DELAY_S):
    logging.info('Downloading %s : START' % url)
    try_count = 1
    start_time = time.time()
    while try_count <= retries:
        try:
            utils.download_file(url, dest)
            end_time = time.time()
            logging.info('Downloading %s : END Elapsed time: %f' % (url, end_time - start_time))
            return True
        except (HTTPError, URLError) as e:
            logging.error(
                'Error in downloading %s Attempt %d : %s . Retrying in %d seconds' % (url, try_count, e.message, delay))
            try_count += 1
            time.sleep(delay)

    raise UnableToDownloadGfsData(url)


class InventoryDownloadThread(threading.Thread):
    def __init__(self, thread_id, url, dest, retries, delay):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.url = url
        self.dest = dest
        self.retries = retries
        self.delay = delay

    def run(self):
        try:
            logging.debug('Downloading from thread %d: START' % self.thread_id)
            download_single_inventory(self.url, self.dest, self.retries, self.delay)
            logging.debug('Downloading from thread %d: END' % self.thread_id)
        except UnableToDownloadGfsData:
            logging.error('Error in downloading from thread %d' % self.thread_id)


def download_gfs_data(wrf_conf):
    logging.info('Downloading GFS data: START')

    if wrf_conf.get('gfs_clean'):
        logging.info('Cleaning the GFS dir: %s' % wrf_conf.get('gfs_dir'))
        utils.cleanup_dir(wrf_conf.get('gfs_dir'))

    gfs_date, gfs_cycle, start_inv = utils.get_appropriate_gfs_inventory(wrf_conf)

    inventories = utils.get_gfs_inventory_url_dest_list(gfs_date, wrf_conf.get('period'), wrf_conf.get('gfs_url'),
                                                        wrf_conf.get('gfs_inv'), wrf_conf.get('gfs_step'),
                                                        gfs_cycle, wrf_conf.get('gfs_res'),
                                                        wrf_conf.get('gfs_dir'), start=start_inv)
    gfs_threads = wrf_conf.get('gfs_threads')
    logging.info(
        'Following data will be downloaded in %d parallel threads\n%s' % (gfs_threads, '\n'.join(
            ' '.join(map(str, i)) for i in inventories)))

    start_time = time.time()
    utils.download_parallel(inventories, procs=gfs_threads, retries=wrf_conf.get('gfs_retries'),
                            delay=wrf_conf.get('gfs_delay'), secondary_dest_dir=None)
    # todo: fix this
    # secondary_dest_dir=utils.get_nfs_gfs_dir(wrf_conf.get('nfs_dir')))

    elapsed_time = time.time() - start_time
    logging.info('Downloading GFS data: END Elapsed time: %f' % elapsed_time)

    logging.info('Downloading GFS data: END')

    return gfs_date, start_inv


def test_download_gfs_data():
    wrf_home = '/tmp/wrf'
    gfs_dir = wrf_home + '/gfs'
    utils.create_dir_if_not_exists(wrf_home)
    utils.create_dir_if_not_exists(gfs_dir)
    conf = get_wrf_config(wrf_home, start_date='2017-08-27_00:00', gfs_dir=gfs_dir, period=0.25)

    gfs_date, start_inv = download_gfs_data(conf)
    logging.info('gfs date %s and start inventory %s' % (gfs_date, start_inv))

    files = os.listdir(gfs_dir)

    assert len(files) == int(24 * conf.get('period') / conf.get('gfs_step')) + 1


def check_gfs_data_availability(date, wrf_config):
    logging.info('Checking gfs data availability...')
    inventories = utils.get_gfs_inventory_dest_list(date, wrf_config.get('period'), wrf_config.get('gfs_inv'),
                                                    wrf_config.get('gfs_step'), wrf_config.get('gfs_cycle'),
                                                    wrf_config.get('gfs_res'), wrf_config.get('gfs_dir'))
    missing_inv = []
    for inv in inventories:
        if not os.path.exists(inv):
            missing_inv.append(inv)

    if len(missing_inv) > 0:
        logging.error('Some data unavailable')
        raise GfsDataUnavailable('Some data unavailable', missing_inv)

    logging.info('GFS data available')


def check_geogrid_output(wps_dir):
    for i in range(1, 4):
        if not os.path.exists(os.path.join(wps_dir, 'geo_em.d%02d.nc' % i)):
            return False
    return True


def run_wps(wrf_config):
    logging.info('Running WPS: START')
    wrf_home = wrf_config.get('wrf_home')
    wps_dir = utils.get_wps_dir(wrf_home)
    output_dir = utils.create_dir_if_not_exists(
        os.path.join(wrf_config.get('nfs_dir'), 'results', wrf_config.get('run_id'), 'wps'))

    logging.info('Backup the output dir')
    utils.backup_dir(output_dir)

    logs_dir = utils.create_dir_if_not_exists(os.path.join(output_dir, 'logs'))

    logging.info('Cleaning up files')
    utils.delete_files_with_prefix(wps_dir, 'FILE:*')
    utils.delete_files_with_prefix(wps_dir, 'PFILE:*')
    utils.delete_files_with_prefix(wps_dir, 'met_em*')

    # Linking VTable
    if not os.path.exists(os.path.join(wps_dir, 'Vtable')):
        logging.info('Creating Vtable symlink')
    os.symlink(os.path.join(wps_dir, 'ungrib/Variable_Tables/Vtable.NAM'), os.path.join(wps_dir, 'Vtable'))

    # Running link_grib.csh
    gfs_date, gfs_cycle, start = utils.get_appropriate_gfs_inventory(wrf_config)
    dest = utils.get_gfs_data_url_dest_tuple(wrf_config.get('gfs_url'), wrf_config.get('gfs_inv'), gfs_date, gfs_cycle,
                                             '', wrf_config.get('gfs_res'), '')[1].replace('.grb2', '')
    utils.run_subprocess(
        'csh link_grib.csh %s/%s' % (wrf_config.get('gfs_dir'), dest), cwd=wps_dir)

    try:
        # Starting ungrib.exe
        try:
            utils.run_subprocess('./ungrib.exe', cwd=wps_dir)
        finally:
            utils.move_files_with_prefix(wps_dir, 'ungrib.log', logs_dir)

        # Starting geogrid.exe'
        if not check_geogrid_output(wps_dir):
            logging.info('Geogrid output not available')
            try:
                utils.run_subprocess('./geogrid.exe', cwd=wps_dir)
            finally:
                utils.move_files_with_prefix(wps_dir, 'geogrid.log', logs_dir)

        # Starting metgrid.exe'
        try:
            utils.run_subprocess('./metgrid.exe', cwd=wps_dir)
        finally:
            utils.move_files_with_prefix(wps_dir, 'metgrid.log', logs_dir)
    finally:
        logging.info('Moving namelist wps file')
        utils.move_files_with_prefix(wps_dir, 'namelist.wps', output_dir)

    logging.info('Running WPS: DONE')

    logging.info('Zipping metgrid data')
    metgrid_zip = os.path.join(wps_dir, wrf_config.get('run_id') + '_metgrid.zip')
    utils.create_zip_with_prefix(wps_dir, 'met_em.d*', metgrid_zip)

    logging.info('Moving metgrid data')
    dest_dir = os.path.join(wrf_config.get('nfs_dir'), 'metgrid')
    utils.move_files_with_prefix(wps_dir, metgrid_zip, dest_dir)


def replace_namelist_wps(wrf_config, start_date=None, end_date=None):
    logging.info('Replacing namelist.wps...')
    if os.path.exists(wrf_config.get('namelist_wps')):
        f = wrf_config.get('namelist_wps')
    else:
        f = res_mgr.get_resource_path(os.path.join('execution', constants.DEFAULT_NAMELIST_WPS_TEMPLATE))

    dest = os.path.join(utils.get_wps_dir(wrf_config.get('wrf_home')), 'namelist.wps')
    replace_file_with_values(wrf_config, f, dest, 'namelist_wps_dict', start_date, end_date)


def replace_namelist_input(wrf_config, start_date=None, end_date=None):
    logging.info('Replacing namelist.input ...')
    if os.path.exists(wrf_config.get('namelist_input')):
        f = wrf_config.get('namelist_input')
    else:
        f = res_mgr.get_resource_path(os.path.join('execution', constants.DEFAULT_NAMELIST_INPUT_TEMPLATE))

    dest = os.path.join(utils.get_em_real_dir(wrf_config.get('wrf_home')), 'namelist.input')
    replace_file_with_values(wrf_config, f, dest, 'namelist_input_dict', start_date, end_date)


def replace_file_with_values(wrf_config, src, dest, aux_dict, start_date=None, end_date=None):
    if start_date is None:
        start_date = utils.datetime_floor(dt.datetime.strptime(wrf_config.get('start_date'), '%Y-%m-%d_%H:%M'),
                                          wrf_config.get('gfs_step') * 3600)

    if end_date is None:
        end_date = start_date + dt.timedelta(days=wrf_config.get('period'))

    period = wrf_config.get('period')

    d = {
        'YYYY1': start_date.strftime('%Y'),
        'MM1': start_date.strftime('%m'),
        'DD1': start_date.strftime('%d'),
        'hh1': start_date.strftime('%H'),
        'mm1': start_date.strftime('%M'),
        'YYYY2': end_date.strftime('%Y'),
        'MM2': end_date.strftime('%m'),
        'DD2': end_date.strftime('%d'),
        'hh2': end_date.strftime('%H'),
        'mm2': end_date.strftime('%M'),
        'GEOG': wrf_config.get('geog_dir'),
        'RD0': str(int(period)),
        'RH0': str(int(period * 24 % 24)),
        'RM0': str(int(period * 60 * 24 % 60)),
        'hi1': '180',
        'hi2': '60',
        'hi3': '60',
    }

    if aux_dict and wrf_config.is_set(aux_dict):
        d.update(wrf_config.get(aux_dict))

    utils.replace_file_with_values(src, dest, d)


def run_em_real(wrf_config):
    logging.info('Running em_real...')

    wrf_home = wrf_config.get('wrf_home')
    em_real_dir = utils.get_em_real_dir(wrf_home)
    procs = wrf_config.get('procs')
    run_id = wrf_config.get('run_id')
    output_dir = utils.create_dir_if_not_exists(os.path.join(wrf_config.get('nfs_dir'), 'results', run_id, 'wrf'))
    archive_dir = utils.create_dir_if_not_exists(os.path.join(wrf_config.get('archive_dir'), 'results', run_id, 'wrf'))

    logging.info('Backup the output dir')
    utils.backup_dir(output_dir)

    logs_dir = utils.create_dir_if_not_exists(os.path.join(output_dir, 'logs'))

    logging.info('Copying metgrid.zip')
    metgrid_dir = os.path.join(wrf_config.get('nfs_dir'), 'metgrid')
    if wrf_config.is_set('wps_run_id'):
        logging.info('wps_run_id is set. Copying metgrid from ' + wrf_config.get('wps_run_id'))
        utils.copy_files_with_prefix(metgrid_dir, wrf_config.get('wps_run_id') + '_metgrid.zip', em_real_dir)
        metgrid_zip = os.path.join(em_real_dir, wrf_config.get('wps_run_id') + '_metgrid.zip')
    else:
        utils.copy_files_with_prefix(metgrid_dir, wrf_config.get('run_id') + '_metgrid.zip', em_real_dir)
        metgrid_zip = os.path.join(em_real_dir, wrf_config.get('run_id') + '_metgrid.zip')

    logging.info('Extracting metgrid.zip')
    ZipFile(metgrid_zip, 'r', compression=ZIP_DEFLATED).extractall(path=em_real_dir)

    # logs destination: nfs/logs/xxxx/rsl*
    try:
        try:
            logging.info('Starting real.exe')
            utils.run_subprocess('mpirun --allow-run-as-root -np %d ./real.exe' % procs, cwd=em_real_dir)
        finally:
            logging.info('Moving Real log files...')
            utils.create_zip_with_prefix(em_real_dir, 'rsl*', os.path.join(em_real_dir, 'real_rsl.zip'), clean_up=True)
            utils.move_files_with_prefix(em_real_dir, 'real_rsl.zip', logs_dir)

        try:
            logging.info('Starting wrf.exe')
            utils.run_subprocess('mpirun --allow-run-as-root -np %d ./wrf.exe' % procs, cwd=em_real_dir)
        finally:
            logging.info('Moving WRF log files...')
            utils.create_zip_with_prefix(em_real_dir, 'rsl*', os.path.join(em_real_dir, 'wrf_rsl.zip'), clean_up=True)
            utils.move_files_with_prefix(em_real_dir, 'wrf_rsl.zip', logs_dir)
    finally:
        logging.info('Moving namelist input file')
        utils.move_files_with_prefix(em_real_dir, 'namelist.input', output_dir)

    logging.info('WRF em_real: DONE! Moving data to the output dir')

    logging.info('Extracting rf from domain3')
    d03_nc = glob.glob(os.path.join(em_real_dir, 'wrfout_d03_*'))[0]
    ncks_query = 'ncks -v %s %s %s' % ('RAINC,RAINNC,XLAT,XLONG,Times', d03_nc, d03_nc + '_rf')
    utils.run_subprocess(ncks_query)

    logging.info('Extracting rf from domain1')
    d01_nc = glob.glob(os.path.join(em_real_dir, 'wrfout_d01_*'))[0]
    ncks_query = 'ncks -v %s %s %s' % ('RAINC,RAINNC,XLAT,XLONG,Times', d01_nc, d01_nc + '_rf')
    utils.run_subprocess(ncks_query)

    logging.info('Moving data to the output dir')
    utils.move_files_with_prefix(em_real_dir, 'wrfout_d03*_rf', output_dir)
    utils.move_files_with_prefix(em_real_dir, 'wrfout_d01*_rf', output_dir)
    logging.info('Moving data to the archive dir')
    utils.move_files_with_prefix(em_real_dir, 'wrfout_*', archive_dir)

    logging.info('Cleaning up files')
    utils.delete_files_with_prefix(em_real_dir, 'met_em*')
    utils.delete_files_with_prefix(em_real_dir, 'rsl*')
    os.remove(metgrid_zip)


# def run_wrf(date, wrf_config):
#     end = date + dt.timedelta(days=wrf_config.get('period'))
#
#     logging.info('Running WRF from %s to %s...' % (date.strftime('%Y%m%d'), end.strftime('%Y%m%d')))
#
#     wrf_home = wrf_config.get('wrf_home')
#     check_gfs_data_availability(date, wrf_config)
#
#     replace_namelist_wps(wrf_config, date, end)
#     run_wps(wrf_home, date)
#
#     replace_namelist_input(wrf_config, date, end)
#     run_em_real(wrf_home, date, wrf_config.get('procs'))
#
#     logging.info('Moving the WRF files to output directory')
#     utils.move_files_with_prefix(utils.get_em_real_dir(wrf_home), 'wrfout_d*', utils.get_output_dir(wrf_home))


# def run_all(wrf_conf, start_date, end_date):
#     logging.info('Running WRF model from %s to %s' % (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
#
#     logging.info('WRF conf\n %s' % wrf_conf.to_string())
#
#     dates = np.arange(start_date, end_date, dt.timedelta(days=1)).astype(dt.datetime)
#
#     for date in dates:
#         logging.info('Creating GFS context')
#         logging.info('Downloading GFS Data for %s period %d' % (date.strftime('%Y-%m-%d'), wrf_conf.get('period')))
#         download_gfs_data(date, wrf_conf)
#
#         logging.info('Running WRF %s period %d' % (date.strftime('%Y-%m-%d'), wrf_conf.get('period')))
#         run_wrf(date, wrf_conf)


class UnableToDownloadGfsData(Exception):
    def __init__(self, msg):
        self.msg = msg
        Exception.__init__(self, 'Unable to download %s' % msg)


class GfsDataUnavailable(Exception):
    def __init__(self, msg, missing_data):
        self.msg = msg
        self.missing_data = missing_data
        Exception.__init__(self, 'Unable to download %s' % msg)


class WrfConfig:
    def __init__(self, configs=None):
        if configs is None:
            configs = {}
        self.configs = configs

    def set_all(self, config_dict):
        self.configs.update(config_dict)

    def set(self, key, value):
        self.configs[key] = value

    def get(self, key):
        return self.configs[key]

    def get_with_defaults(self, key, default_val):
        try:
            return self.configs[key]
        except KeyError:
            return default_val

    def get_all(self):
        return self.configs.copy()

    def to_string(self):
        return str(self.configs)

    def to_json_string(self):
        return json.dumps(self.configs, sort_keys=True)

    def is_set(self, key):
        return key in self.configs

    def is_empty(self):
        return len(self.configs) == 0

    def __str__(self):
        return str(self.configs)


def get_wrf_config(wrf_home, config_file=None, start_date=None, **kwargs):
    """
    precedence = kwargs > wrf_config.json > constants
    """
    conf = get_default_wrf_config(wrf_home)

    if config_file is not None and os.path.exists(config_file):
        with open(config_file, 'r') as f:
            conf_json = json.load(f)
            conf.set_all(conf_json['wrf_config'])

    if start_date is not None:
        conf.set('start_date', start_date)

    for key in kwargs:
        conf.set(key, kwargs[key])

    return conf


def get_default_wrf_config(wrf_home=constants.DEFAULT_WRF_HOME):
    defaults = {'wrf_home': wrf_home,
                'nfs_dir': utils.get_nfs_dir(wrf_home),
                'geog_dir': utils.get_geog_dir(wrf_home),
                'gfs_dir': utils.get_gfs_dir(constants.DEFAULT_WRF_HOME, False),
                'period': constants.DEFAULT_PERIOD,
                'offset': constants.DEFAULT_OFFSET,
                'namelist_input': constants.DEFAULT_NAMELIST_INPUT_TEMPLATE,
                # 'namelist_input_dict': constants.DEFAULT_NAMELIST_INPUT_TEMPLATE_DICT,
                'namelist_wps': constants.DEFAULT_NAMELIST_WPS_TEMPLATE,
                # 'namelist_wps_dict': constants.DEFAULT_NAMELIST_WPS_TEMPLATE_DICT,
                'procs': constants.DEFAULT_PROCS,
                'gfs_clean': True,
                'gfs_cycle': constants.DEFAULT_CYCLE,
                'gfs_delay': constants.DEFAULT_DELAY_S,
                'gfs_inv': constants.DEFAULT_GFS_DATA_INV,
                'gfs_res': constants.DEFAULT_RES,
                'gfs_retries': constants.DEFAULT_RETRIES,
                'gfs_step': constants.DEFAULT_STEP,
                'gfs_url': constants.DEFAULT_GFS_DATA_URL,
                'gfs_lag': constants.DEFAULT_GFS_LAG_HOURS,
                'gfs_threads': constants.DEFAULT_THREAD_COUNT}

    return WrfConfig(defaults)


class TestExecutor(unittest.TestCase):
    def test_replace_file_with_values(self):
        wrf_output_dir = tempfile.mkdtemp(prefix='test_replace_file_with_values_')

        content = """
        YYYY1
        MM1
        DD1
        hh1
        mm1
        YYYY2
        MM2
        DD2
        hh2
        mm2
        GEOG
        RD0
        RH0
        RM0
        hi1
        hi2
        hi3
        test1"""

        in_txt = os.path.join(wrf_output_dir, 'in.txt')
        out_txt = os.path.join(wrf_output_dir, 'out.txt')
        with open(in_txt, 'w') as f:
            f.write(content)

        wc = WrfConfig(
            {'start_date': '2017-01-02_12:34',
             'gfs_step': 3,
             'period': 3,
             'geog_dir': 'abcd',
             'aux': {
                 'hi3': '10',
                 'test1': 'xxx'
             }
             })

        replace_file_with_values(wc, in_txt, out_txt, 'aux')


if __name__ == "__main__":
    pass
