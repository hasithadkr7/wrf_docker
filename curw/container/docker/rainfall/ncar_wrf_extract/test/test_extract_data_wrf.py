import os
import shutil
import tempfile
import unittest

import logging

from curw.rainfall.wrf import utils
from curw.rainfall.wrf.resources import manager as res_mgr
from curw.container.docker.rainfall.ncar_wrf_extract import extract_data_wrf


class TestExtractDataWrf(unittest.TestCase):
    def test_all(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')

        wrf_home = tempfile.mkdtemp(prefix='wrf_test_')
        nfs_dir = os.path.join(wrf_home, 'nfs')

        run_id = 'WRF_test0'

        output_dir = os.path.join(nfs_dir, 'results', run_id, 'wrf')
        utils.create_dir_if_not_exists(output_dir)
        shutil.copy(res_mgr.get_resource_path('test/wrfout_d03_2017-10-02_12:00:00'), output_dir)

        wrf_conf_dict = {
            "wrf_home": wrf_home,
            "nfs_dir": nfs_dir,
            "period": 0.25,
            "start_date": "2017-10-02_12:00"
        }

        db_conf_dict = {
            "host": "localhost",
            "user": "test",
            "password": "password",
            "db": "testdb"
        }

        extract_data_wrf.run(run_id, wrf_conf_dict, db_conf_dict, upsert=True)


def suite():
    s = unittest.TestSuite()
    s.addTest(TestExtractDataWrf)
    return s
