DEFAULT_WRF_HOME = '/mnt/disks/wrf-mod/'
DEFAULT_NFS_DIR = '/mnt/disks/wrf-mod/DATA/nfs'
DEFAULT_GEOG_DIR = '/mnt/disks/wrf-mod/DATA/geog'


## GFS configs
# DEFAULT_GFS_DATA_URL = 'ftp://ftpprd.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.YYYYMMDDCC/'
DEFAULT_GFS_DATA_URL = 'http://www.ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/gfs.YYYYMMDDCC/'
DEFAULT_GFS_DATA_INV = 'gfs.tCCz.pgrb2.RRRR.fFFF'
DEFAULT_GFS_LAG_HOURS = 4
DEFAULT_THREAD_COUNT = 8
DEFAULT_RETRIES = 5
DEFAULT_DELAY_S = 60
DEFAULT_CYCLE = '00'
DEFAULT_RES = '0p50'
DEFAULT_PERIOD = 3
DEFAULT_STEP = 3


# DEFAULT_EM_REAL_PATH = 'WRFV3/test/em_real/'
DEFAULT_EM_REAL_PATH = 'WRFV3/run/'
DEFAULT_WPS_PATH = 'WPS/'
DEFAULT_PROCS = 4
DEFAULT_OFFSET = 0
DEFAULT_NAMELIST_INPUT_TEMPLATE = 'namelist.input'
# DEFAULT_NAMELIST_INPUT_TEMPLATE_DICT = 'namelist.input'
DEFAULT_NAMELIST_WPS_TEMPLATE = 'namelist.wps'
# DEFAULT_NAMELIST_WPS_TEMPLATE_DICT = {
#         'YYYY1': start_date.strftime('%Y'),
#         'MM1': start_date.strftime('%m'),
#         'DD1': start_date.strftime('%d'),
#         'YYYY2': end_date.strftime('%Y'),
#         'MM2': end_date.strftime('%m'),
#         'DD2': end_date.strftime('%d'),
#         'GEOG': utils.get_geog_dir(wrf_config.get('wrf_home'))
#     }


LOGGING_ENV_VAR = 'LOG_YAML'
