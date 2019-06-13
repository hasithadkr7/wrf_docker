#!/usr/bin/env python

import sys
import logging
import os


def update_kelani_raincell_file(raincell_file_dir, factor, output_file):
    lines = open(os.path.join(raincell_file_dir, 'RAINCELL.DAT')).read().split("\n")
    out = open(os.path.join(raincell_file_dir, output_file), 'w')

    for i, line in enumerate(lines):
        line = line.strip()
        if not line:  # line is blank
            continue
        if i == 0:
            out.write(line + '\n')
        else:
            splits = line.split(' ')
            out.write('%s %f\n' % (splits[0], float(splits[1]) * factor))
    out.close()


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(module)s %(levelname)s %(message)s')

    date = argv[1]
    logging.info('input file %s' % date)

    factor = float(argv[2])
    logging.info('correction factor %s' % str(factor))

    dest_file = argv[3] if len(argv) > 3 else 'RAINCELL.DAT.UPDATED'
    logging.info('dest file %s' % dest_file)

    # rf_file = argv[4] if len(argv) > 4 else '/home/uwcc-admin/jaxa-data-mgt/summary.txt'
    # logging.info('rf file %s' % rf_file)
    #
    # raincell_dir = argv[5] if len(argv) > 5 else ''
    # logging.info('rf file %s' % rf_file)

    update_kelani_raincell_file(date, factor, dest_file)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
