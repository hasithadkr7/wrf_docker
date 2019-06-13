#!/bin/python
import datetime as dt

from curw.rainfall.wrf import utils


def main():
    args_dict = utils.parse_args()

    wrf_home = args_dict.pop('wrf_home')
    start_date = dt.datetime.strptime(args_dict.pop('start'), '%Y-%m-%d_%H:%M')
    end_date = dt.datetime.strptime(args_dict.pop('end'), '%Y-%m-%d_%H:%M')
    wrf_config_file = args_dict.pop('wrf_config')

    utils.set_logging_config(utils.get_logs_dir(wrf_home))

    # wrf_conf = executor.get_wrf_config(wrf_home, config_file=wrf_config_file, **args_dict)

    # executor.run_all(wrf_conf, start_date, end_date)
    #
    # extractor.extract_all(wrf_home, start_date, end_date)


if __name__ == "__main__":
    main()
