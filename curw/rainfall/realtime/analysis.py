import datetime as dt
import logging as log
import os
import re

import numpy as np
from curw.rainfall.wrf import utils


def read_sat_data_files(sat_data_home):
    log.info('Satallite data reading...')
    names = ["TIMESTAMP", "RECORD", "STATION", "Rain_Tot"]
    convertfunc = lambda x: dt.datetime.strptime(x.strip('"'), '%Y-%m-%d %H:%M:%S')

    sat_data = None
    for f in os.listdir(sat_data_home):
        log.info('Reading %s' % f)
        station = re.search('KALU\d*', f).group(0)
        current = np.genfromtxt(os.path.join(sat_data_home, f), skip_header=4, names=names, delimiter=',',
                                usecols=(0, 1, 2, 3), dtype=None, converters={0: convertfunc, 2: lambda s: station})
        if sat_data is not None:
            sat_data = np.unique(np.hstack((sat_data, current)))
        else:
            sat_data = current

    log.info('Satallite data read: Done')
    return sat_data


def main():
    log_home = '/tmp'
    utils.set_logging_config(log_home)
    sat_home = '/home/nira/Desktop/jaxa/'
    sat_data = read_sat_data_files(sat_home + '/DATA_REAL_TIME')
    log.info('Done')


if __name__ == "__main__":
    main()
