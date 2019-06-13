import argparse
import datetime as dt
import errno
import glob
import json
import logging
import logging.config
import math
import multiprocessing
import ntpath
import os
import re
import shlex
import shutil
import signal
import subprocess
import time
from functools import wraps
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from zipfile import ZipFile, ZIP_DEFLATED

import pkg_resources
from curw.rainfall.wrf import constants
from joblib import Parallel, delayed
from shapely.geometry import Point, shape


def parse_args(parser_description='Running WRF'):
    def t_or_f(arg):
        ua = str(arg).upper()
        if 'TRUE'.startswith(ua):
            return True
        elif 'FALSE'.startswith(ua):
            return False
        else:
            raise argparse.ArgumentTypeError('Boolean value expected.')

    parser = argparse.ArgumentParser(description=parser_description)
    parser.add_argument('-start', default=dt.datetime.today().strftime('%Y-%m-%d_%H:%M'),
                        help='Start timestamp with format %%Y-%%m-%%d_%%H:%%M', dest='start')
    parser.add_argument('-end', default=(dt.datetime.today() + dt.timedelta(days=1)).strftime('%Y-%m-%d_%H:%M'),
                        help='End timestamp with format %%Y-%%m-%%d_%%H:%%M', dest='end')
    parser.add_argument('-wrf_config', default=pkg_resources.resource_filename(__name__, 'wrf_config.json'),
                        help='Path to the wrf_config.json', dest='wrf_config')

    conf_group = parser.add_argument_group('wrf_config', 'Arguments for WRF config')
    conf_group.add_argument('-wrf_home', '-wrf', default=constants.DEFAULT_WRF_HOME, help='WRF home', dest='wrf_home')
    conf_group.add_argument('-period', help='Model running period in days', type=int)
    conf_group.add_argument('-namelist_input', help='namelist.input file path')
    conf_group.add_argument('-namelist_wps', help='namelist.wps file path')
    conf_group.add_argument('-procs', help='Num. of processors for WRF run', type=int)
    conf_group.add_argument('-gfs_dir', help='GFS data dir path')
    conf_group.add_argument('-gfs_clean', type=t_or_f, help='If true, gfs_dir will be cleaned before downloading data')
    conf_group.add_argument('-gfs_inv', help='GFS inventory format. default = gfs.tCCz.pgrb2.RRRR.fFFF')
    conf_group.add_argument('-gfs_res', help='GFS inventory resolution. default = 0p50')
    conf_group.add_argument('-gfs_step', help='GFS time step (in hours) between data sets', type=int)
    conf_group.add_argument('-gfs_retries', help='GFS num. of retries for each download', type=int)
    conf_group.add_argument('-gfs_delay', help='GFS delay between retries', type=int)
    conf_group.add_argument('-gfs_url', help='GFS URL')
    conf_group.add_argument('-gfs_threads', help='GFS num. of parallel downloading threads', type=int)

    # remove all the arguments which are None
    args_dict = dict((k, v) for k, v in list(dict(parser.parse_args()._get_kwargs()).items()) if v)

    return args_dict


def set_logging_config(log_home):
    default_config = dict(
        version=1,
        formatters={
            'f': {'format': '%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s'}
        },
        handlers={
            'h': {'class': 'logging.StreamHandler',
                  'formatter': 'f',
                  'level': logging.INFO},
            'fh': {'class': 'logging.handlers.TimedRotatingFileHandler',
                   'formatter': 'f',
                   'level': logging.INFO,
                   'filename': os.path.join(log_home, 'wrfrun.log'),
                   'when': 'D',
                   'interval': 1
                   }
        },
        root={
            'handlers': ['h', 'fh'],
            'level': logging.INFO,
        },
    )

    logging_config_path = pkg_resources.resource_filename(__name__, 'logging.json')
    value = os.getenv(constants.LOGGING_ENV_VAR, None)

    if value:
        logging_config_path = value
    if os.path.exists(logging_config_path):
        with open(logging_config_path, 'rt') as f:
            config = json.load(f)
        config['handlers']['fh']['filename'] = os.path.join(log_home, 'wrfrun.log')
        logging.config.dictConfig(config)
    else:
        logging.config.dictConfig(default_config)


def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def get_nfs_gfs_dir(nfs_home=constants.DEFAULT_NFS_DIR, create_dir=True):
    if create_dir:
        return create_dir_if_not_exists(os.path.join(nfs_home, 'data', 'gfs'))
    else:
        return os.path.join(nfs_home, 'data', 'gfs')


def get_nfs_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return os.path.join(wrf_home, 'DATA', 'nfs')


def get_gfs_dir(wrf_home=constants.DEFAULT_WRF_HOME, create_dir=True):
    if create_dir:
        return create_dir_if_not_exists(os.path.join(wrf_home, 'DATA', 'GFS'))
    else:
        return os.path.join(wrf_home, 'DATA', 'GFS')


def get_wps_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return os.path.join(wrf_home, constants.DEFAULT_WPS_PATH)


def get_em_real_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return os.path.join(wrf_home, constants.DEFAULT_EM_REAL_PATH)


def get_geog_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return os.path.join(wrf_home, 'DATA', 'geog')


def get_output_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return create_dir_if_not_exists(os.path.join(wrf_home, 'OUTPUT'))


def get_scripts_run_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return create_dir_if_not_exists(os.path.join(wrf_home, 'wrf-scripts', 'run'))


def get_logs_dir(wrf_home=constants.DEFAULT_WRF_HOME):
    return create_dir_if_not_exists(os.path.join(wrf_home, 'logs'))


def get_gfs_data_url_dest_tuple(url, inv, date_str, cycle, fcst_id, res, gfs_dir):
    url0 = url.replace('YYYY', date_str[0:4]).replace('MM', date_str[4:6]).replace('DD', date_str[6:8]).replace('CC',
                                                                                                                cycle)
    inv0 = inv.replace('CC', cycle).replace('FFF', fcst_id).replace('RRRR', res).replace('YYYY', date_str[0:4]).replace(
        'MM', date_str[4:6]).replace('DD', date_str[6:8])

    dest = os.path.join(gfs_dir, date_str + '.' + inv0)
    return url0 + inv0, dest


def get_gfs_data_dest(inv, date_str, cycle, fcst_id, res, gfs_dir):
    inv0 = inv.replace('CC', cycle).replace('FFF', fcst_id).replace('RRRR', res)
    dest = os.path.join(gfs_dir, date_str + '.' + inv0)
    return dest


def get_gfs_inventory_url_dest_list(date, period, url, inv, step, cycle, res, gfs_dir, start=0):
    date_str = date.strftime('%Y%m%d') if type(date) is dt.datetime else date
    return [get_gfs_data_url_dest_tuple(url, inv, date_str, cycle, str(i).zfill(3), res, gfs_dir) for i in
            range(start, start + int(period * 24) + 1, step)]


def get_gfs_inventory_dest_list(date, period, inv, step, cycle, res, gfs_dir):
    date_str = date.strftime('%Y%m%d')
    return [get_gfs_data_dest(inv, date_str, cycle, str(i).zfill(3), res, gfs_dir) for i in
            range(0, period * 24 + 1, step)]


def replace_file_with_values(source, destination, val_dict):
    logging.debug('replace file source ' + source)
    logging.debug('replace file destination ' + destination)
    logging.debug('replace file content dict ' + str(val_dict))

    # pattern = re.compile(r'\b(' + '|'.join(val_dict.keys()) + r')\b')
    pattern = re.compile('|'.join(list(val_dict.keys())))

    with open(destination, 'w') as dest:
        out = ''
        with open(source, 'r') as src:
            line = pattern.sub(lambda x: val_dict[x.group()], src.read())
            dest.write(line)
            out += line

    logging.debug('replace file final content \n' + out)


def cleanup_dir(path):
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path)


def delete_files_with_prefix(src_dir, prefix):
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        os.remove(filename)


def copy_files_with_prefix(src_dir, prefix, dest_dir):
    create_dir_if_not_exists(dest_dir)
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        shutil.copy(filename, os.path.join(dest_dir, ntpath.basename(filename)))


def copy_if_not_exists(src, dest):
    if not file_exists_nonempty(dest):
        return shutil.copy2(src, dest)
    else:
        return dest


def move_files_with_prefix(src_dir, prefix, dest_dir):
    create_dir_if_not_exists(dest_dir)
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        shutil.move(filename, os.path.join(dest_dir, ntpath.basename(filename)))


def create_symlink_with_prefix(src_dir, prefix, dest_dir):
    for filename in glob.glob(os.path.join(src_dir, prefix)):
        os.symlink(filename, os.path.join(dest_dir, ntpath.basename(filename)))


# def create_zipfile(file_list, output, compression=ZIP_DEFLATED):
#     with ZipFile(output, 'w', compression=compression) as z:
#         for file in file_list:
#             z.write(file)


def create_zip_with_prefix(src_dir, regex, dest_zip, comp=ZIP_DEFLATED, clean_up=False):
    with ZipFile(dest_zip, 'w', compression=comp) as zip_file:
        for filename in glob.glob(os.path.join(src_dir, regex)):
            zip_file.write(filename, arcname=os.path.basename(filename))
            if clean_up:
                os.remove(filename)
    return dest_zip


def run_subprocess(cmd, cwd=None, print_stdout=False):
    logging.info('Running subprocess %s cwd %s' % (cmd, cwd))
    start_t = time.time()
    output = ''
    try:
        output = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT, cwd=cwd)
    except subprocess.CalledProcessError as e:
        logging.error('Exception in subprocess %s! Error code %d' % (cmd, e.returncode))
        logging.error(e.output)
        raise e
    finally:
        elapsed_t = time.time() - start_t
        logging.info('Subprocess %s finished in %f s' % (cmd, elapsed_t))
        if print_stdout:
            logging.info('stdout and stderr of %s\n%s' % (cmd, output))
    return output


def is_inside_polygon(polygons, lat, lon):
    point = Point(lon, lat)
    for i, poly in enumerate(polygons.shapeRecords()):
        polygon = shape(poly.shape.__geo_interface__)
        if point.within(polygon):
            return 1
    return 0


class TimeoutError(Exception):
    def __init__(self, msg, timeout_s):
        self.msg = msg
        self.timeout_s = timeout_s
        Exception.__init__(self, 'Unable to download %s' % msg)


def timeout(seconds=600, error_message=os.strerror(errno.ETIME)):
    """
    if a method exceeds 600s (10 min) raise a timer expired error
    source: https://stackoverflow.com/questions/2281850/timeout-function-if-it-takes-too-long-to-finish
    errno.ETIME Timer expired
    """

    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message, seconds)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator


def datetime_to_epoch(timestamp=None):
    timestamp = dt.datetime.now() if timestamp is None else timestamp
    return (timestamp - dt.datetime(1970, 1, 1)).total_seconds()


def epoch_to_datetime(epoch_time):
    return dt.datetime(1970, 1, 1) + dt.timedelta(seconds=epoch_time)


def datetime_floor(timestamp, floor_sec):
    return epoch_to_datetime(math.floor(datetime_to_epoch(timestamp) / floor_sec) * floor_sec)


def datetime_lk_to_utc(timestamp_lk,  shift_mins=0):
    return timestamp_lk - dt.timedelta(hours=5, minutes=30 + shift_mins)


def datetime_utc_to_lk(timestamp_utc, shift_mins=0):
    return timestamp_utc + dt.timedelta(hours=5, minutes=30 + shift_mins)


def file_exists_nonempty(filename):
    return os.path.exists(filename) and os.path.isfile(filename) and os.stat(filename).st_size != 0


def download_file(url, dest, retries=0, delay=60, overwrite=False, secondary_dest_dir=None):
    try_count = 1
    last_e = None

    def _download_file(_url, _dest):
        _f = urlopen(_url)
        with open(_dest, "wb") as _local_file:
            _local_file.write(_f.read())

    while try_count <= retries + 1:
        try:
            logging.info("Downloading %s to %s" % (url, dest))
            if secondary_dest_dir is None:
                if not overwrite and file_exists_nonempty(dest):
                    logging.info('File already exists. Skipping download!')
                else:
                    _download_file(url, dest)
                return
            else:
                secondary_file = os.path.join(secondary_dest_dir, os.path.basename(dest))
                if file_exists_nonempty(secondary_file):
                    logging.info("File available in secondary dir. Copying to the destination dir from secondary dir")
                    shutil.copyfile(secondary_file, dest)
                else:
                    logging.info("File not available in secondary dir. Downloading...")
                    _download_file(url, dest)
                    logging.info("Copying to the secondary dir")
                    shutil.copyfile(dest, secondary_file)
                return

        except (HTTPError, URLError) as e:
            logging.error(
                'Error in downloading %s Attempt %d : %s . Retrying in %d seconds' % (url, try_count, e.message, delay))
            try_count += 1
            last_e = e
            time.sleep(delay)
        except FileExistsError:
            logging.info('File was already downloaded by another process! Returning')
            return

    raise last_e


def download_parallel(url_dest_list, procs=multiprocessing.cpu_count(), retries=0, delay=60, overwrite=False,
                      secondary_dest_dir=None):
    Parallel(n_jobs=procs)(
        delayed(download_file)(i[0], i[1], retries, delay, overwrite, secondary_dest_dir) for i in url_dest_list)


def get_appropriate_gfs_inventory(wrf_config):
    st = datetime_floor(dt.datetime.strptime(wrf_config.get('start_date'), '%Y-%m-%d_%H:%M'), 3600 * wrf_config.get(
        'gfs_step'))
    # if the time difference between now and start time is lt gfs_lag, then the time will be adjusted
    if (dt.datetime.utcnow() - st).total_seconds() <= wrf_config.get('gfs_lag') * 3600:
        floor_val = datetime_floor(st - dt.timedelta(hours=wrf_config.get('gfs_lag')), 6 * 3600)
    else:
        floor_val = datetime_floor(st, 6 * 3600)
    gfs_date = floor_val.strftime('%Y%m%d')
    gfs_cycle = str(floor_val.hour).zfill(2)
    start_inv = math.floor((st - floor_val).total_seconds() / 3600 / wrf_config.get('gfs_step')) * wrf_config.get(
        'gfs_step')

    return gfs_date, gfs_cycle, start_inv


def get_incremented_dir_path(path):
    """
    returns the incremented dir path
    ex: /a/b/c/0 if not exists returns /a/b/c/0 else /a/b/c/1
    :param path:
    :return:
    """
    while os.path.exists(path):
        try:
            base = str(int(os.path.basename(path)) + 1)
            path = os.path.join(os.path.dirname(path), base)
        except ValueError:
            path = os.path.join(path, '0')

    return path


def backup_dir(path):
    bck_str = '__backup'
    if os.path.exists(path):
        bck_files = [l for l in os.listdir(path) if bck_str not in l]
        if len(bck_files) > 0:
            bck_dir = get_incremented_dir_path(os.path.join(path, bck_str))
            os.makedirs(bck_dir)
            for file in bck_files:
                shutil.move(os.path.join(path, file), bck_dir)
            return bck_dir

    return None


# def namedtuple_with_defaults(typename, field_names, default_values=()):
#     T = namedtuple(typename, field_names)
#     T.__new__.__defaults__ = (None,) * len(T._fields)
#     if isinstance(default_values, collections.Mapping):
#         prototype = T(**default_values)
#     else:
#         prototype = T(*default_values)
#     T.__new__.__defaults__ = tuple(prototype)
#     return T


# def ncdump(nc_fid, verb=True):
#     def print_ncattr(key):
#         try:
#             print "\t\ttype:", repr(nc_fid.variables[key].dtype)
#             for ncattr in nc_fid.variables[key].ncattrs():
#                 print '\t\t%s:' % ncattr, \
#                     repr(nc_fid.variables[key].getncattr(ncattr))
#         except KeyError:
#             print "\t\tWARNING: %s does not contain variable attributes" % key
#
#     # NetCDF global attributes
#     _nc_attrs = nc_fid.ncattrs()
#     if verb:
#         print "NetCDF Global Attributes:"
#         for nc_attr in _nc_attrs:
#             print '\t%s:' % nc_attr, repr(nc_fid.getncattr(nc_attr))
#     _nc_dims = [dim for dim in nc_fid.dimensions]  # list of nc dimensions
#     # Dimension shape information.
#     if verb:
#         print "NetCDF dimension information:"
#         for dim in _nc_dims:
#             print "\tName:", dim
#             print "\t\tsize:", len(nc_fid.dimensions[dim])
#             print_ncattr(dim)
#     # Variable information.
#     _nc_vars = [var for var in nc_fid.variables]  # list of nc variables
#     if verb:
#         print "NetCDF variable information:"
#         for var in _nc_vars:
#             if var not in _nc_dims:
#                 print '\tName:', var
#                 print "\t\tdimensions:", nc_fid.variables[var].dimensions
#                 print "\t\tsize:", nc_fid.variables[var].size
#                 print_ncattr(var)
#     return _nc_attrs, _nc_dims, _nc_vars


def main():
    wrf_home = "/tmp"
    set_logging_config(wrf_home)
    print(get_gfs_dir(wrf_home))
    print(get_output_dir(wrf_home))
    print(get_scripts_run_dir(wrf_home))
    print(get_logs_dir(wrf_home))
    print(get_gfs_data_url_dest_tuple(constants.DEFAULT_GFS_DATA_URL, constants.DEFAULT_GFS_DATA_INV,
                                      get_gfs_dir(wrf_home), '20170501', constants.DEFAULT_CYCLE, '001',
                                      constants.DEFAULT_RES))
    print(get_gfs_inventory_url_dest_list(constants.DEFAULT_GFS_DATA_URL, constants.DEFAULT_GFS_DATA_INV,
                                          dt.datetime.strptime('2017-05-01', '%Y-%m-%d'),
                                          constants.DEFAULT_PERIOD,
                                          constants.DEFAULT_STEP,
                                          constants.DEFAULT_CYCLE,
                                          constants.DEFAULT_RES,
                                          get_gfs_dir(wrf_home)))
    d = {
        'YYYY1': '2016',
        'MM1': '05',
        'DD1': '01'
    }

    print(replace_file_with_values('resources/namelist.input', wrf_home + '/namelist.wps', d))


if __name__ == "__main__":
    main()
