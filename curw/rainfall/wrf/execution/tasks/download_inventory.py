#!/usr/bin/python

import argparse
import logging
import ntpath
import os
import shutil

from curw.rainfall.wrf.execution import executor
from curw.rainfall.wrf import utils


def parse_args():
    parser = argparse.ArgumentParser(description='Downloading single inventory')
    parser.add_argument('url', dest='url')
    parser.add_argument('dest', dest='url')

    return parser.parse_args()


def download_single_inventory_task(url, dest):
    logging.info('Downloading from %s to %s' % (url, dest))
    executor.download_single_inventory(url, dest, retries=1, delay=0)


def download_i_th_inventory(i, gfs_url, gfs_inv, gfs_date, gfs_cycle, gfs_res, gfs_dir, nfs_dir, test_mode=False):
    logging.info('Downloading %d inventory' % i)

    url, dest = utils.get_gfs_data_url_dest_tuple(gfs_url, gfs_inv, gfs_date, gfs_cycle, str(i).zfill(3), gfs_res,
                                                  gfs_dir)
    dest_nfs = os.path.join(utils.get_nfs_gfs_dir(nfs_dir), ntpath.basename(dest))
    logging.info('URL %s, dest %s, nfs_des %s' % (url, dest, dest_nfs))

    if not test_mode:
        if os.path.exists(dest_nfs) and os.path.isfile(dest_nfs) and os.stat(dest_nfs).st_size != 0:
            logging.info("File available in NFS. Copying to the GFS dir from NFS")
            shutil.copyfile(dest_nfs, dest)
        else:
            logging.info("File not available in NFS. Downloading...")
            executor.download_single_inventory(url, dest, retries=1, delay=0)
            shutil.copyfile(dest, dest_nfs)
    else:
        logging.info('Running on test mode')


class DownloadSingleInventoryTaskException(Exception):
    def __init__(self, msg):
        self.msg = msg
        Exception.__init__(self, msg)


if __name__ == "__main__":
    args = parse_args()
    if args.url is None or args.dest is None:
        raise DownloadSingleInventoryTaskException(
            'Unable to download data: url=%s dest= %s' % (str(args.url), str(args.dest)))

        # download_single_inventory_task(args.url, args.dest)
