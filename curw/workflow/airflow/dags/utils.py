import datetime as dt
import logging
import os
import time

from airflow.models import Variable
from curw.rainfall.wrf.execution import executor as wrf_exec
from curw.rainfall.wrf import constants, utils


# def get_gfs_download_subdag(parent_dag_name, child_dag_name, args, wrf_config_key='wrf_config', test_mode=False):
#     dag_subdag = DAG(
#         dag_id='%s.%s' % (parent_dag_name, child_dag_name),
#         default_args=args,
#         schedule_interval=None,
#     )
#
#     try:
#         wrf_config = WrfConfig(configs=Variable.get(wrf_config_key, deserialize_json=True))
#     except KeyError as e:
#         logging.error('Key error %s' % str(e))
#         return dag_subdag
#
#     period = wrf_config.get('period')
#     step = wrf_config.get('gfs_step')
#
#     try:
#         gfs_date, gfs_cycle, start = utils.get_appropriate_gfs_inventory(wrf_config)
#     except KeyError as e:
#         # raise WrfRunException(str(e))
#         logging.error('Unable to find the key: %s. Returining an empty subdag' % str(e))
#         return dag_subdag
#
#     for i in range(int(start), int(start) + period * 24 + 1, step):
#         PythonOperator(
#             python_callable=download_inventory.download_i_th_inventory,
#             task_id='%s-task-%s' % (child_dag_name, i),
#             op_args=[i, wrf_config.get('gfs_url'), wrf_config.get('gfs_inv'), gfs_date, gfs_cycle,
#                      wrf_config.get('gfs_res'), wrf_config.get('gfs_dir'), wrf_config.get('nfs_dir'), test_mode],
#             # provide_context=True,
#             default_args=args,
#             dag=dag_subdag,
#         )
#
#     return dag_subdag
#
#
# def get_initial_parameters_subdag(parent_dag_name, child_dag_name, runs, args, wrf_home_key, wrf_start_date_key,
#                                   wrf_config_key):
#     dag_subdag = DAG(
#         dag_id='%s.%s' % (parent_dag_name, child_dag_name),
#         default_args=args,
#         schedule_interval=None,
#     )
#
#     for i in [str(x) for x in range(runs)]:
#         PythonOperator(
#             task_id='%s-task-%s' % (child_dag_name, i),
#             python_callable=set_initial_parameters_fs,
#             provide_context=True,
#             op_args=[wrf_home_key + i, wrf_start_date_key + i, wrf_config_key + i],
#             default_args=args,
#             dag=dag_subdag,
#         )
#
#     return dag_subdag
#
#
# def get_wrf_run_subdag(parent_dag_name, child_dag_name, runs, args, wrf_config_key, test_mode = False):
#     dag_subdag = DAG(
#         dag_id='%s.%s' % (parent_dag_name, child_dag_name),
#         default_args=args,
#         schedule_interval=None,
#     )
#
#     for i in [str(x) for x in range(runs)]:
#         real = CurwPythonOperator(
#             task_id='%s-task-%s-%s' % (child_dag_name, 'real', i),
#             curw_task=tasks.Real,
#             init_args=[wrf_config_key + i],
#             provide_context=True,
#             default_args=args,
#             dag=dag_subdag,
#             test_mode=test_mode
#         )
#
#         wrf = CurwPythonOperator(
#             task_id='%s-task-%s-%s' % (child_dag_name, 'wrf', i),
#             curw_task=tasks.Wrf,
#             init_args=[wrf_config_key + i],
#             provide_context=True,
#             default_args=args,
#             dag=dag_subdag,
#             test_mode=test_mode
#         )
#
#         real >> wrf
#
#     return dag_subdag


def set_initial_parameters_fs(wrf_home_key='wrf_home', wrf_start_date_key='wrf_start_date', wrf_config_key='wrf_config',
                              ignore_namelists=False, **kwargs):
    # set wrf_home --> wrf_home Var > WRF_HOME env var > wrf_home default
    try:
        wrf_home = Variable.get(wrf_home_key)
    except KeyError:
        try:
            wrf_home = os.environ['WRF_HOME']
        except KeyError:
            wrf_home = constants.DEFAULT_WRF_HOME
    logging.info('wrf_home: %s' % wrf_home)

    # set wrf_config --> wrf_config Var (YAML format) > get_wrf_config(wrf_home)
    try:
        wrf_config_dict = Variable.get(wrf_config_key, deserialize_json=True)
        wrf_config = wrf_exec.get_wrf_config(wrf_config_dict.pop('wrf_home'), **wrf_config_dict)
    except KeyError:
        wrf_config = wrf_exec.get_wrf_config(wrf_home)
        logging.warning('%s Variable is set to %s' % (wrf_config_key, wrf_config.to_json_string()))
    logging.info('wrf_config: %s' % wrf_config.to_string())

    if wrf_home != wrf_config.get('wrf_home'):
        logging.warning('wrf_home and wrf_config[wrf_home] are different! Please check the inputs')

    # set start_date --> wrf_start_date var > execution_date param in the workflow > today
    start_date_dt = None
    start_date = None
    try:
        start_date_dt_utc = dt.datetime.strptime(Variable.get(wrf_start_date_key), '%Y-%m-%d_%H:%M') + dt.timedelta(
            seconds=time.altzone)
        start_date_dt = utils.datetime_floor(start_date_dt_utc, 3600)
    except KeyError as e1:
        logging.warning('wrf_start_date variable is not available. execution_date will be used - %s' % str(e1))
        try:
            start_date_dt = utils.datetime_floor(kwargs['execution_date'] + dt.timedelta(seconds=time.altzone), 3600)
            # NOTE: airflow execution time is at the end of the (starting day + interval) period
            start_date_dt = start_date_dt + dt.timedelta(days=1)
        except KeyError as e2:
            logging.warning('execution_date is not available - %s' % str(e2))

    if start_date_dt is not None:
        start_date = (start_date_dt - dt.timedelta(hours=wrf_config.get('offset'))).strftime('%Y-%m-%d_%H:%M')
        logging.info('wrf_start_date: %s' % start_date)

    if start_date is not None and (not wrf_config.is_set('start_date') or wrf_config.get('start_date') != start_date):
        logging.info('Setting start date ' + start_date)
        wrf_config.set('start_date', start_date)
        # date_splits = re.split('[-_:]', start_date)
        Variable.set(wrf_config_key, wrf_config.to_json_string())
        logging.info('New wrf conifg: ' + wrf_config.to_json_string())

    if not ignore_namelists:
        logging.info('Replacing namelist.wps place-holders')
        wrf_exec.replace_namelist_wps(wrf_config)

        logging.info('Replacing namelist.input place-holders')
        wrf_exec.replace_namelist_input(wrf_config)

    if 'ti' in kwargs:
        kwargs['ti'].xcom_push(key=wrf_config_key, value=wrf_config.to_json_string())


class WrfRunException(Exception):
    def __init__(self, msg):
        self.msg = msg
        Exception.__init__(self, msg)


if __name__ == "__main__":
    pass
