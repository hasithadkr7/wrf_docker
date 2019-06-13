import argparse
import ast
import datetime as dt
import logging
import multiprocessing
import os
import shutil
import tempfile
import zipfile
from random import random

import matplotlib
import numpy as np
from curw.rainfall.wrf.extraction import utils as ext_utils
from joblib import Parallel, delayed
from mpl_toolkits.basemap import cm
from curw.rainfall.wrf import utils
from curwmysqladapter import Station

matplotlib.use('Agg')
from matplotlib import colors, pyplot as plt


def extract_jaxa_satellite_hourly_data(ts, output_dir):
    ts = utils.datetime_floor(ts, 3600)
    logging.info('Jaxa satellite data extraction for %s (previous hour)' % str(ts))
    extract_jaxa_satellite_data(ts - dt.timedelta(hours=1), ts, output_dir)


def create_daily_gif(start, output_dir, output_filename, output_prefix):
    tmp_dir = tempfile.mkdtemp(prefix='tmp_jaxa_daily')

    utils.copy_files_with_prefix(output_dir, output_prefix + '_' + start.strftime('%Y-%m-%d') + '*.png', tmp_dir)
    logging.info('Writing gif ' + output_filename)
    gif_list = [os.path.join(tmp_dir, i) for i in sorted(os.listdir(tmp_dir))]
    if len(gif_list) > 0:
        ext_utils.create_gif(gif_list, os.path.join(output_dir, output_filename))
    else:
        logging.info('No images found to create the gif')

    logging.info('Cleaning up ' + tmp_dir)
    shutil.rmtree(tmp_dir)


def extract_jaxa_satellite_data(start_ts_utc, end_ts_utc, output_dir, cleanup=True, cum=False, tmp_dir=None,
                                lat_min=5.722969, lon_min=79.52146, lat_max=10.06425, lon_max=82.18992,
                                output_prefix='jaxa_sat', db_adapter_config=None):
    start = utils.datetime_floor(start_ts_utc, 3600)
    end = utils.datetime_floor(end_ts_utc, 3600)

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
        for k, v in list(ph.items()):
            _url = _url.replace(k, v)
        return _url

    # tmp_dir = os.path.join(output_dir, 'tmp_jaxa/')
    # if not os.path.exists(tmp_dir):
    #     os.mkdir(tmp_dir)
    if tmp_dir is None:
        tmp_dir = tempfile.mkdtemp(prefix='tmp_jaxa')

    url_dest_list = []
    for timestamp in np.arange(start, end, dt.timedelta(hours=1)).astype(dt.datetime):
        url = get_jaxa_url(timestamp)
        url_dest_list.append((url, os.path.join(tmp_dir, os.path.basename(url)),
                              os.path.join(output_dir,
                                           output_prefix + '_' + timestamp.strftime('%Y-%m-%d_%H:%M') + '.asc')))

    procs = multiprocessing.cpu_count()

    logging.info('Downloading inventory in parallel')
    utils.download_parallel(url_dest_list, procs)
    logging.info('Downloading inventory complete')

    logging.info('Processing files in parallel')
    Parallel(n_jobs=procs)(
        delayed(process_jaxa_zip_file)(i[1], i[2], lat_min, lon_min, lat_max, lon_max, cum, output_prefix,
                                       db_adapter_config) for i in url_dest_list)
    logging.info('Processing files complete')

    logging.info('Creating sat rf gif for today')
    create_daily_gif(start, output_dir, output_prefix + '_today.gif', output_prefix)

    prev_day_gif = os.path.join(output_dir, output_prefix + '_yesterday.gif')
    if not utils.file_exists_nonempty(prev_day_gif) or start.strftime('%H:%M') == '00:00':
        logging.info('Creating sat rf gif for yesterday')
        create_daily_gif(utils.datetime_floor(start, 3600 * 24) - dt.timedelta(days=1), output_dir,
                         output_prefix + '_yesterday.gif', output_prefix)

    if cum:
        logging.info('Processing cumulative')
        process_cumulative_plot(url_dest_list, start_ts_utc, end_ts_utc, output_dir, lat_min, lon_min, lat_max, lon_max)
        logging.info('Processing cumulative complete')

    # clean up temp dir
    if cleanup:
        logging.info('Cleaning up')
        shutil.rmtree(tmp_dir)
        utils.delete_files_with_prefix(output_dir, '*.archive')


def test_extract_jaxa_satellite_data():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    end = dt.datetime.utcnow() - dt.timedelta(hours=6)
    start = end - dt.timedelta(hours=1)

    extract_jaxa_satellite_data(start, end, '/home/curw/temp/jaxa', cleanup=False, tmp_dir='/home/curw/temp/jaxa/data')


def test_extract_jaxa_satellite_data_with_db():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    end = dt.datetime.utcnow() - dt.timedelta(hours=3)
    start = end - dt.timedelta(hours=1)

    config = {
        "host": "localhost",
        "user": "test",
        "password": "password",
        "db": "testdb"
    }
    extract_jaxa_satellite_data(start, end, '/home/nira/temp1/jaxa', cleanup=False,
                                tmp_dir='/home/nira/temp1/jaxa/data',
                                db_adapter_config=config)


def test_extract_jaxa_satellite_data_d01():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    end = dt.datetime.utcnow() - dt.timedelta(hours=1)
    start = end - dt.timedelta(hours=1)

    lat_min = -3.06107
    lon_min = 71.2166
    lat_max = 18.1895
    lon_max = 90.3315

    extract_jaxa_satellite_data(start, end, '/tmp/jaxa', cleanup=False, tmp_dir='/tmp/jaxa/data',
                                lat_min=lat_min, lat_max=lat_max, lon_min=lon_min, lon_max=lon_max, output_prefix='abc')


def process_cumulative_plot(url_dest_list, start_ts_utc, end_ts_utc, output_dir, lat_min, lon_min, lat_max, lon_max):
    from_to = '%s-%s' % (start_ts_utc.strftime('%Y-%m-%d_%H:%M'), end_ts_utc.strftime('%Y-%m-%d_%H:%M'))
    cum_filename = os.path.join(output_dir, 'jaxa_sat_cum_rf_' + from_to + '.png')

    if not utils.file_exists_nonempty(cum_filename):
        total = None
        for url_dest in url_dest_list:
            if total is None:
                total = np.genfromtxt(url_dest[2] + '.archive', dtype=float)
            else:
                total += np.genfromtxt(url_dest[2] + '.archive', dtype=float)
        title = 'Cumulative rainfall ' + from_to
        # clevs = np.concatenate(([-1, 0], np.array([pow(2, i) for i in range(0, 9)])))
        clevs_cum = 10 * np.array([0.1, 0.5, 1, 2, 3, 5, 10, 15, 20, 25, 30, 50, 75, 100])
        norm_cum = colors.BoundaryNorm(boundaries=clevs_cum, ncolors=256)
        cmap = plt.get_cmap('jet')
        ext_utils.create_contour_plot(total, cum_filename, lat_min, lon_min, lat_max, lon_max, title, clevs=clevs_cum,
                                      cmap=cmap, norm=norm_cum)
    else:
        logging.info('%s already exits' % cum_filename)


def process_jaxa_zip_file(zip_file_path, out_file_path, lat_min, lon_min, lat_max, lon_max, archive_data=False,
                          output_prefix='jaxa_sat', db_adapter_config=None):
    sat_zip = zipfile.ZipFile(zip_file_path)
    sat = np.genfromtxt(sat_zip.open(os.path.basename(zip_file_path).replace('.zip', '')), delimiter=',', names=True)
    sat_filt = np.sort(
        sat[(sat['Lat'] <= lat_max) & (sat['Lat'] >= lat_min) & (sat['Lon'] <= lon_max) & (sat['Lon'] >= lon_min)],
        order=['Lat', 'Lon'])
    lats = np.sort(np.unique(sat_filt['Lat']))
    lons = np.sort(np.unique(sat_filt['Lon']))

    data = sat_filt['RainRate'].reshape(len(lats), len(lons))

    ext_utils.create_asc_file(np.flip(data, 0), lats, lons, out_file_path)

    # clevs = np.concatenate(([-1, 0], np.array([pow(2, i) for i in range(0, 9)])))
    # clevs = 10 * np.array([0.1, 0.5, 1, 2, 3, 5, 10, 15, 20, 25, 30])
    # norm = colors.BoundaryNorm(boundaries=clevs, ncolors=256)
    # cmap = plt.get_cmap('jet')
    clevs = [0, 1, 2.5, 5, 7.5, 10, 15, 20, 30, 40, 50, 75, 100, 150, 200, 250, 300]
    # clevs = [0.1, 0.5, 1, 2, 3, 5, 10, 15, 20, 25, 30, 50, 75, 100]
    norm = None
    cmap = cm.s3pcpn

    ts = dt.datetime.strptime(os.path.basename(out_file_path).replace(output_prefix + '_', '').replace('.asc', ''),
                              '%Y-%m-%d_%H:%M')
    lk_ts = utils.datetime_utc_to_lk(ts)
    title_opts = {
        'label': output_prefix + ' ' + lk_ts.strftime('%Y-%m-%d %H:%M') + ' LK\n' + ts.strftime(
            '%Y-%m-%d %H:%M') + ' UTC',
        'fontsize': 30
    }
    ext_utils.create_contour_plot(data, out_file_path + '.png', np.min(lats), np.min(lons), np.max(lats), np.max(lons),
                                  title_opts, clevs=clevs, cmap=cmap, norm=norm)

    if archive_data and not utils.file_exists_nonempty(out_file_path + '.archive'):
        np.savetxt(out_file_path + '.archive', data, fmt='%g')
    else:
        logging.info('%s already exits' % (out_file_path + '.archive'))

    if not db_adapter_config:
        logging.info('db_adapter not available. Unable to push data!')
        return

    db_adapter = ext_utils.get_curw_adapter(mysql_config=db_adapter_config)

    width = len(lons)
    height = len(lats)
    station_prefix = 'sat'
    run_name = 'Cloud-1'
    upsert = True

    def random_check_stations_exist():
        for _ in range(10):
            _x = lons[int(random() * width)]
            _y = lats[int(random() * height)]
            _name = '%s_%.6f_%.6f' % (station_prefix, _x, _y)
            _query = {'name': _name}
            if db_adapter.get_station(_query) is None:
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
                station = [Station.Sat, station_id, name, str(lon), str(lat), str(0), "WRF point"]
                db_adapter.create_station(station)

            # add rf series to the dict
            rf_ts[name] = [[lk_ts.strftime('%Y-%m-%d %H:%M:%S'), data[y, x]]]

    ext_utils.push_rainfall_to_db(db_adapter, rf_ts, source=station_prefix, name=run_name, types=['Observed'],
                                  upsert=upsert)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')
    parser = argparse.ArgumentParser(description='Run all stages of WRF')
    parser.add_argument('-start',
                        default=(dt.datetime.utcnow() - dt.timedelta(days=1, hours=1)).strftime('%Y-%m-%d_%H:%M'),
                        help='Start timestamp UTC with format %%Y-%%m-%%d_%%H:%%M', dest='start_ts')
    parser.add_argument('-end', default=(dt.datetime.utcnow() - dt.timedelta(hours=1)).strftime('%Y-%m-%d_%H:%M'),
                        help='End timestamp UTC with format %%Y-%%m-%%d_%%H:%%M', dest='end_ts')
    parser.add_argument('-output', default=None, help='Output directory of the images', dest='output')
    parser.add_argument('-prefix', default='jaxa_sat', help='Output prefix')
    parser.add_argument('-clean', default=1, help='Cleanup temp directory', dest='clean', type=int)
    parser.add_argument('-cum', default=0, help='Process cumulative plot', dest='cum', type=int)

    parser.add_argument('-lat_min', default=5.722969, help='Lat min', type=float)
    parser.add_argument('-lon_min', default=79.52146, help='lon min', type=float)
    parser.add_argument('-lat_max', default=10.06425, help='Lat max', type=float)
    parser.add_argument('-lon_max', default=82.18992, help='Lon max', type=float)

    parser.add_argument('-db_config', default='{}', help='DB config of to push sat data', dest='db_config')

    args = parser.parse_args()

    if args.output is None:
        output = os.path.join(utils.get_output_dir(), 'jaxa_sat')
    else:
        output = args.output

    db_config_dict = ast.literal_eval(args.db_config)
    # adapter = ext_utils.get_curw_adapter(mysql_config=db_config_dict) if db_config_dict else None

    extract_jaxa_satellite_data(dt.datetime.strptime(args.start_ts, '%Y-%m-%d_%H:%M'),
                                dt.datetime.strptime(args.end_ts, '%Y-%m-%d_%H:%M'),
                                output, cleanup=bool(args.clean), cum=bool(args.cum), lat_min=args.lat_min,
                                lon_min=args.lon_min, lat_max=args.lat_max, lon_max=args.lon_max,
                                output_prefix=args.prefix, db_adapter_config=db_config_dict)
