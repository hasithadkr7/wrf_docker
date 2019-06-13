import datetime as dt
import json
import logging
import os
import shutil
import zipfile
from zipfile import ZipFile

import numpy as np
from airflow.models import Variable
from curw.rainfall.wrf.execution.executor import WrfConfig
from curw.rainfall.wrf.execution import executor
from mpl_toolkits.basemap import Basemap, cm
from curw.rainfall.wrf import utils
from curw.rainfall.wrf.extraction import wt_extractor, utils as ext_utils


class CurwTask(object):
    def __init__(self, config=WrfConfig()):
        self.config = config

    def pre_process(self, *args, **kwargs):
        pass

    def post_process(self, *args, **kwargs):
        pass

    def process(self, *args, **kwargs):
        pass

    def get_config(self, **kwargs):
        if self.config.is_empty():
            self.set_config(**kwargs)
        return self.config

    def set_config(self, **kwargs):
        raise NotImplementedError('Provide a way to get the config!')


class WrfTask(CurwTask):
    def __init__(self, wrf_config_key='wrf_config', **kwargs):
        self.wrf_config_key = wrf_config_key
        super(WrfTask, self).__init__(**kwargs)

    def set_config(self, **kwargs):
        wrf_config_json = None
        if 'ti' in kwargs:
            wrf_config_json = kwargs['ti'].xcom_pull(task_ids=None, key=self.wrf_config_key)

        if wrf_config_json is not None:
            logging.info('wrf_config from xcom using %s key: %s' % (self.wrf_config_key, wrf_config_json))
            self.config = WrfConfig(json.loads(wrf_config_json))
        else:
            try:
                self.config = WrfConfig(Variable.get(self.wrf_config_key, deserialize_json=True))
                logging.info(
                    'wrf_config from variable using %s key: %s' % (self.wrf_config_key, self.config.to_json_string))
            except KeyError:
                raise CurwAriflowTasksException('Unable to find WrfConfig')

    def add_config_item(self, key, value):
        self.config.set(key, value)
        Variable.set(self.wrf_config_key, self.config.to_json_string())


class Ungrib(WrfTask):
    def pre_process(self, *args, **kwargs):
        logging.info('Running preprocessing for ungrib...')

        wrf_config = self.get_config(**kwargs)

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

    def process(self, *args, **kwargs):
        logging.info('Running ungrib...')

        wrf_config = self.get_config(**kwargs)
        logging.info('wrf conifg: ' + wrf_config.to_json_string())

        wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))

        # Running link_grib.csh
        logging.info('Running link_grib.csh')
        gfs_date, gfs_cycle, start = utils.get_appropriate_gfs_inventory(wrf_config)
        # use get_gfs_data_url_dest_tuple to get
        dest = \
            utils.get_gfs_data_url_dest_tuple(wrf_config.get('gfs_url'), wrf_config.get('gfs_inv'), gfs_date, gfs_cycle,
                                              '', wrf_config.get('gfs_res'), '')[1]

        utils.run_subprocess(
            'csh link_grib.csh %s/%s' % (wrf_config.get('gfs_dir'), dest), cwd=wps_dir)

        utils.run_subprocess('./ungrib.exe', cwd=wps_dir)


class FindWeatherType(WrfTask):
    def __init__(self, wrf_config_keys, wt_namelists, wt_namelists_path):
        self.wrf_config_keys = wrf_config_keys
        self.wt_namelists = wt_namelists
        self.wt_namelists_path = wt_namelists_path
        super(FindWeatherType, self).__init__(wrf_config_key=wrf_config_keys[0])

    def process(self, *args, **kwargs):
        wrf_config0 = self.get_config()
        logging.info('wrf conifg: ' + wrf_config0.to_json_string())

        gfs_date, gfs_cycle, start = utils.get_appropriate_gfs_inventory(wrf_config0)

        inv_24 = utils.get_gfs_data_url_dest_tuple(wrf_config0.get('gfs_url'), wrf_config0.get('gfs_inv'), gfs_date,
                                                   gfs_cycle, str(start + 24).zfill(3), wrf_config0.get('gfs_res'),
                                                   wrf_config0.get('gfs_dir'))
        wt = wt_extractor.get_weather_type(inv_24[1])
        logging.info('Weather type in 24h %s' % wt)

        wt_var = Variable.get(self.wt_namelists, deserialize_json=True)

        logging.info('Deprecated: Changing the wrf_configs according to the weather type')
        logging.info('Using default weather types')
        wt = 'default'
        wt_namelists = wt_var[wt]
        for i in range(len(self.wrf_config_keys)):
            logging.info('Setting ' + self.wrf_config_keys[i])
            var = Variable.get(self.wrf_config_keys[i], deserialize_json=True)
            var['namelist_input'] = os.path.join(self.wt_namelists_path, wt_namelists[i])
            logging.info('New %s: %s' % (self.wrf_config_keys[i], str(var)))
            Variable.set(self.wrf_config_keys[i], var, serialize_json=True)


class Metgrid(WrfTask):
    def pre_process(self, *args, **kwargs):
        logging.info('Running pre-processing for metgrid...')

        wrf_config = self.get_config(**kwargs)
        wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))
        utils.delete_files_with_prefix(wps_dir, 'met_em*')

    def process(self, *args, **kwargs):
        logging.info('Running metgrid...')
        wrf_config = self.get_config(**kwargs)
        logging.info('wrf conifg: ' + wrf_config.to_json_string())

        wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))

        utils.run_subprocess('./metgrid.exe', cwd=wps_dir)

    def post_process(self, *args, **kwargs):
        # make a sym link in the nfs dir
        wrf_config = self.get_config(**kwargs)
        wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))

        nfs_metgrid_dir = os.path.join(wrf_config.get('nfs_dir'), 'metgrid')

        utils.create_dir_if_not_exists(nfs_metgrid_dir)
        # utils.delete_files_with_prefix(nfs_metgrid_dir, 'met_em.d*')
        # utils.create_symlink_with_prefix(wps_dir, 'met_em.d*', nfs_metgrid_dir)

        utils.create_zip_with_prefix(wps_dir, 'met_em.d*', os.path.join(wps_dir, 'metgrid.zip'))

        utils.delete_files_with_prefix(nfs_metgrid_dir, 'met_em.d*')
        utils.move_files_with_prefix(wps_dir, 'metgrid.zip', nfs_metgrid_dir)


class Geogrid(WrfTask):
    def process(self, *args, **kwargs):
        logging.info('Running geogrid...')

        wrf_config = self.get_config(**kwargs)
        wps_dir = utils.get_wps_dir(wrf_config.get('wrf_home'))

        if not executor.check_geogrid_output(wps_dir):
            logging.info('Running Geogrid.exe')
            utils.run_subprocess('./geogrid.exe', cwd=wps_dir)
        else:
            logging.info('Geogrid output already available')


class Real(WrfTask):
    def pre_process(self, *args, **kwargs):
        wrf_config = self.get_config(**kwargs)
        wrf_home = wrf_config.get('wrf_home')
        nfs_metgrid_dir = os.path.join(wrf_config.get('nfs_dir'), 'metgrid')

        logging.info('Running em_real...')
        em_real_dir = utils.get_em_real_dir(wrf_home)

        logging.info('Cleaning up files')
        utils.delete_files_with_prefix(em_real_dir, 'met_em*')
        utils.delete_files_with_prefix(em_real_dir, 'rsl*')

        # Linking met_em.*
        # logging.info('Creating met_em.d* symlinks')
        logging.info('Copying met_em.d.zip file')
        utils.copy_files_with_prefix(nfs_metgrid_dir, 'metgrid.zip', em_real_dir)

        logging.info('Unzipping met_em.d.zip')
        ZipFile(os.path.join(em_real_dir, 'metgrid.zip'), 'r', compression=zipfile.ZIP_DEFLATED).extractall(
            path=em_real_dir)

        logging.info('Cleaning up met_em.d.zip')
        os.remove(os.path.join(em_real_dir, 'metgrid.zip'))

    def process(self, *args, **kwargs):
        wrf_config = self.get_config(**kwargs)
        logging.info('wrf conifg: ' + wrf_config.to_json_string())

        wrf_home = wrf_config.get('wrf_home')
        em_real_dir = utils.get_em_real_dir(wrf_home)
        procs = wrf_config.get('procs')
        utils.run_subprocess('mpirun -np %d ./real.exe' % procs, cwd=em_real_dir)

    def post_process(self, *args, **kwargs):
        wrf_home = self.get_config(**kwargs).get('wrf_home')
        start_date = self.get_config(**kwargs).get('start_date')
        em_real_dir = utils.get_em_real_dir(wrf_home)

        logging.info('Moving the real logs')
        utils.move_files_with_prefix(em_real_dir, 'rsl*', os.path.join(utils.get_logs_dir(wrf_home),
                                                                       'rsl-real-%s' % start_date))


class Wrf(WrfTask):
    def process(self, *args, **kwargs):
        wrf_config = self.get_config(**kwargs)
        logging.info('wrf conifg: ' + wrf_config.to_json_string())

        wrf_home = wrf_config.get('wrf_home')
        em_real_dir = utils.get_em_real_dir(wrf_home)
        procs = wrf_config.get('procs')

        # logging.info('Replacing namelist.input place-holders')
        # executor.replace_namelist_input(wrf_config)

        utils.run_subprocess('mpirun -np %d ./wrf.exe' % procs, cwd=em_real_dir)

    def post_process(self, *args, **kwargs):
        config = self.get_config(**kwargs)
        wrf_home = config.get('wrf_home')
        em_real_dir = utils.get_em_real_dir(wrf_home)
        start_date = config.get('start_date')

        logging.info('Moving the WRF logs')
        utils.move_files_with_prefix(em_real_dir, 'rsl*',
                                     os.path.join(utils.get_logs_dir(wrf_home), 'rsl-wrf-%s' % start_date))

        logging.info('Moving the WRF files to output directory')
        # move the d03 to nfs
        # ex: /mnt/disks/wrf-mod/nfs/output/wrf0/2017-08-13_00:00/0 .. n
        d03_dir = utils.get_incremented_dir_path(
            os.path.join(config.get('nfs_dir'), 'output', os.path.basename(wrf_home), start_date, '0'))
        self.add_config_item('wrf_output_dir', d03_dir)

        d03_file = os.path.join(em_real_dir, 'wrfout_d03_' + start_date + ':00')
        ext_utils.ncks_extract_variables(d03_file, ['RAINC', 'RAINNC', 'XLAT', 'XLONG', 'Times'], d03_file + '_SL')

        d01_file = os.path.join(em_real_dir, 'wrfout_d01_' + start_date + ':00')
        ext_utils.ncks_extract_variables(d01_file, ['RAINC', 'RAINNC', 'XLAT', 'XLONG', 'Times'], d01_file + '_SL')

        # move the wrfout_SL and the namelist files to the nfs
        utils.create_dir_if_not_exists(d03_dir)
        shutil.move(d03_file + '_SL', d03_dir)
        shutil.move(d01_file + '_SL', d03_dir)
        shutil.copy2(os.path.join(em_real_dir, 'namelist.input'), d03_dir)

        # move the rest to the OUTPUT dir of each run
        # todo: in the docker impl - FIND A BETTER WAY
        archive_dir = utils.get_incremented_dir_path(os.path.join(utils.get_output_dir(wrf_home), start_date))
        utils.move_files_with_prefix(em_real_dir, 'wrfout_d*', archive_dir)


class RainfallExtraction(WrfTask):
    def process(self, *args, **kwargs):
        config = self.get_config(**kwargs)
        logging.info('wrf conifg: ' + config.to_json_string())

        start_date = config.get('start_date')
        d03_dir = config.get('wrf_output_dir')
        d03_sl = os.path.join(d03_dir, 'wrfout_d03_' + start_date + ':00_SL')

        # create a temp work dir & get a local copy of the d03.._SL
        temp_dir = utils.create_dir_if_not_exists(os.path.join(config.get('wrf_home'), 'temp'))
        shutil.copy2(d03_sl, temp_dir)

        d03_sl = os.path.join(temp_dir, os.path.basename(d03_sl))

        lat_min = 5.722969
        lon_min = 79.52146
        lat_max = 10.06425
        lon_max = 82.18992

        variables = ext_utils.extract_variables(d03_sl, 'RAINC, RAINNC', lat_min, lat_max, lon_min, lon_max)

        lats = variables['XLAT']
        lons = variables['XLONG']

        # cell size is calc based on the mean between the lat and lon points
        cz = np.round(np.mean(np.append(lons[1:len(lons)] - lons[0: len(lons) - 1], lats[1:len(lats)]
                                        - lats[0: len(lats) - 1])), 3)
        # clevs = 10 * np.array([0.1, 0.5, 1, 2, 3, 5, 10, 15, 20, 25, 30])
        # clevs_cum = 10 * np.array([0.1, 0.5, 1, 2, 3, 5, 10, 15, 20, 25, 30, 50, 75, 100])
        # norm = colors.BoundaryNorm(boundaries=clevs, ncolors=256)
        # norm_cum = colors.BoundaryNorm(boundaries=clevs_cum, ncolors=256)
        # cmap = plt.get_cmap('jet')

        clevs = [0, 1, 2.5, 5, 7.5, 10, 15, 20, 30, 40, 50, 70, 100, 150, 200, 250, 300, 400, 500, 600, 750]
        clevs_cum = clevs
        norm = None
        norm_cum = None
        cmap = cm.s3pcpn

        basemap = Basemap(projection='merc', llcrnrlon=lon_min, llcrnrlat=lat_min, urcrnrlon=lon_max,
                          urcrnrlat=lat_max, resolution='h')

        filter_threshold = 0.05
        data = variables['RAINC'] + variables['RAINNC']
        logging.info('Filtering with the threshold %f' % filter_threshold)
        data[data < filter_threshold] = 0.0
        variables['PRECIP'] = data

        pngs = []
        ascs = []

        for i in range(1, len(variables['Times'])):
            time = variables['Times'][i]
            ts = dt.datetime.strptime(time, '%Y-%m-%d_%H:%M:%S')
            lk_ts = utils.datetime_utc_to_lk(ts)
            logging.info('processing %s', time)

            # instantaneous precipitation (hourly)
            inst_precip = variables['PRECIP'][i] - variables['PRECIP'][i - 1]

            inst_file = os.path.join(temp_dir, 'wrf_inst_' + time)
            title = {
                'label': 'Hourly rf for %s LK\n%s UTC' % (lk_ts.strftime('%Y-%m-%d_%H:%M:%S'), time),
                'fontsize': 30
            }

            ext_utils.create_asc_file(np.flip(inst_precip, 0), lats, lons, inst_file + '.asc', cell_size=cz)
            ascs.append(inst_file + '.asc')

            ext_utils.create_contour_plot(inst_precip, inst_file + '.png', lat_min, lon_min, lat_max, lon_max,
                                          title, clevs=clevs, cmap=cmap, basemap=basemap, norm=norm)
            pngs.append(inst_file + '.png')

            if i % 24 == 0:
                t = 'Daily rf from %s LK to %s LK' % (
                    (lk_ts - dt.timedelta(hours=24)).strftime('%Y-%m-%d_%H:%M:%S'), lk_ts.strftime('%Y-%m-%d_%H:%M:%S'))
                d = int(i / 24) - 1
                logging.info('Creating images for D%d' % d)
                cum_file = os.path.join(temp_dir, 'wrf_cum_%dd' % d)

                ext_utils.create_asc_file(np.flip(variables['PRECIP'][i], 0), lats, lons, cum_file + '.asc',
                                          cell_size=cz)
                ascs.append(cum_file + '.asc')

                ext_utils.create_contour_plot(variables['PRECIP'][i] - variables['PRECIP'][i - 24], cum_file + '.png',
                                              lat_min, lon_min, lat_max, lon_max, t, clevs=clevs, cmap=cmap,
                                              basemap=basemap, norm=norm_cum)
                pngs.append(inst_file + '.png')

                gif_file = os.path.join(temp_dir, 'wrf_inst_%dd' % d)
                images = [os.path.join(temp_dir, 'wrf_inst_' + i.strftime('%Y-%m-%d_%H:%M:%S') + '.png') for i in
                          np.arange(ts - dt.timedelta(hours=23), ts + dt.timedelta(hours=1),
                                    dt.timedelta(hours=1)).astype(dt.datetime)]
                ext_utils.create_gif(images, gif_file + '.gif')

        logging.info('Creating the zips')
        utils.create_zip_with_prefix(temp_dir, '*.png', os.path.join(temp_dir, 'pngs.zip'))
        utils.create_zip_with_prefix(temp_dir, '*.asc', os.path.join(temp_dir, 'ascs.zip'))
        # utils.create_zipfile(pngs, os.path.join(temp_dir, 'pngs.zip'))
        # utils.create_zipfile(ascs, os.path.join(temp_dir, 'ascs.zip'))

        logging.info('Cleaning up instantaneous pngs and ascs - wrf_inst_*')
        utils.delete_files_with_prefix(temp_dir, 'wrf_inst_*.png')
        utils.delete_files_with_prefix(temp_dir, 'wrf_inst_*.asc')

        logging.info('Copying pngs to ' + d03_dir)
        utils.move_files_with_prefix(temp_dir, '*.png', d03_dir)
        logging.info('Copying ascs to ' + d03_dir)
        utils.move_files_with_prefix(temp_dir, '*.asc', d03_dir)
        logging.info('Copying gifs to ' + d03_dir)
        utils.copy_files_with_prefix(temp_dir, '*.gif', d03_dir)
        logging.info('Copying zips to ' + d03_dir)
        utils.copy_files_with_prefix(temp_dir, '*.zip', d03_dir)

        d03_latest_dir = os.path.join(config.get('nfs_dir'), 'latest', os.path.basename(config.get('wrf_home')))
        # <nfs>/latest/wrf0 .. 3
        utils.create_dir_if_not_exists(d03_latest_dir)
        # todo: this needs to be adjusted to handle the multiple runs
        logging.info('Copying gifs to ' + d03_latest_dir)
        utils.copy_files_with_prefix(temp_dir, '*.gif', d03_latest_dir)

        logging.info('Cleaning up temp dir')
        shutil.rmtree(temp_dir)


class RainfallExtractionD01(WrfTask):
    def process(self, *args, **kwargs):
        config = self.get_config(**kwargs)
        logging.info('wrf conifg: ' + config.to_json_string())

        start_date = config.get('start_date')
        d03_dir = config.get('wrf_output_dir')
        d03_sl = os.path.join(d03_dir, 'wrfout_d01_' + start_date + ':00_SL')

        # create a temp work dir & get a local copy of the d03.._SL
        temp_dir = utils.create_dir_if_not_exists(os.path.join(config.get('wrf_home'), 'temp_d01'))
        shutil.copy2(d03_sl, temp_dir)

        d03_sl = os.path.join(temp_dir, os.path.basename(d03_sl))

        lat_min = -3.06107
        lon_min = 71.2166
        lat_max = 18.1895
        lon_max = 90.3315

        variables = ext_utils.extract_variables(d03_sl, 'RAINC, RAINNC', lat_min, lat_max, lon_min, lon_max)

        lats = variables['XLAT']
        lons = variables['XLONG']

        # cell size is calc based on the mean between the lat and lon points
        cz = np.round(np.mean(np.append(lons[1:len(lons)] - lons[0: len(lons) - 1], lats[1:len(lats)]
                                        - lats[0: len(lats) - 1])), 3)
        # clevs = 10 * np.array([0.1, 0.5, 1, 2, 3, 5, 10, 15, 20, 25, 30])
        # clevs_cum = 10 * np.array([0.1, 0.5, 1, 2, 3, 5, 10, 15, 20, 25, 30, 50, 75, 100])
        # norm = colors.BoundaryNorm(boundaries=clevs, ncolors=256)
        # norm_cum = colors.BoundaryNorm(boundaries=clevs_cum, ncolors=256)
        # cmap = plt.get_cmap('jet')

        clevs = [0, 1, 2.5, 5, 7.5, 10, 15, 20, 30, 40, 50, 70, 100, 150, 200, 250, 300, 400, 500, 600, 750]
        clevs_cum = clevs
        norm = None
        cmap = cm.s3pcpn

        basemap = Basemap(projection='merc', llcrnrlon=lon_min, llcrnrlat=lat_min, urcrnrlon=lon_max,
                          urcrnrlat=lat_max, resolution='h')

        filter_threshold = 0.05
        data = variables['RAINC'] + variables['RAINNC']
        logging.info('Filtering with the threshold %f' % filter_threshold)
        data[data < filter_threshold] = 0.0
        variables['PRECIP'] = data

        for i in range(1, len(variables['Times'])):
            time = variables['Times'][i]
            ts = dt.datetime.strptime(time, '%Y-%m-%d_%H:%M:%S')
            lk_ts = utils.datetime_utc_to_lk(ts)
            logging.info('processing %s', time)

            # instantaneous precipitation (hourly)
            inst_precip = variables['PRECIP'][i] - variables['PRECIP'][i - 1]

            inst_file = os.path.join(temp_dir, 'wrf_inst_' + time)
            title = {
                'label': '3Hourly rf for %s LK\n%s UTC' % (lk_ts.strftime('%Y-%m-%d_%H:%M:%S'), time),
                'fontsize': 30
            }
            ext_utils.create_contour_plot(inst_precip, inst_file + '.png', lat_min, lon_min, lat_max, lon_max,
                                          title, clevs=clevs, cmap=cmap, basemap=basemap, norm=norm)

            if i % 8 == 0:
                d = int(i / 8) - 1
                logging.info('Creating gif for D%d' % d)
                gif_file = os.path.join(temp_dir, 'wrf_inst_D01_%dd' % d)
                images = [os.path.join(temp_dir, 'wrf_inst_' + i.strftime('%Y-%m-%d_%H:%M:%S') + '.png') for i in
                          np.arange(ts - dt.timedelta(hours=24 - 3), ts + dt.timedelta(hours=3),
                                    dt.timedelta(hours=3)).astype(dt.datetime)]
                ext_utils.create_gif(images, gif_file + '.gif')

        # move all the data in the tmp dir to the nfs
        logging.info('Copying gifs to ' + d03_dir)
        utils.copy_files_with_prefix(temp_dir, '*.gif', d03_dir)

        d03_latest_dir = os.path.join(config.get('nfs_dir'), 'latest', os.path.basename(config.get('wrf_home')))
        # <nfs>/latest/wrf0 .. 3
        utils.create_dir_if_not_exists(d03_latest_dir)
        # todo: this needs to be adjusted to handle the multiple runs
        logging.info('Copying gifs to ' + d03_latest_dir)
        utils.copy_files_with_prefix(temp_dir, '*.gif', d03_latest_dir)

        logging.info('Cleaning up the dir ' + temp_dir)
        shutil.rmtree(temp_dir)


def test_rainfall_extraction():
    rf_task = RainfallExtraction()
    rf_task.config = WrfConfig({
        'wrf_home': '/home/curw/Desktop/temp',
        'wrf_output_dir': '/home/curw/Desktop/temp',
        'start_date': '2017-08-13_00:00'
    })

    rf_task.process()


def test_rainfall_extraction2():
    rf_task = RainfallExtractionD01()
    rf_task.config = WrfConfig({
        'wrf_home': '/home/nira/Desktop/temp',
        'wrf_output_dir': '/home/nira/Desktop/temp',
        'nfs_dir': '/home/nira/Desktop/temp',
        'start_date': '2017-09-24_00:00'
    })

    rf_task.process()


class CurwAriflowTasksException(Exception):
    def __init__(self, msg):
        self.msg = msg
        Exception.__init__(self, msg)
