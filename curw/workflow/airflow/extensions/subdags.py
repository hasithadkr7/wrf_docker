import logging
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from curw.rainfall.wrf.execution.executor import WrfConfig
from curw.rainfall.wrf.execution.tasks import download_inventory
from curw.workflow.airflow.extensions.operators.curw_python_operator import CurwPythonOperator
from curw.workflow.airflow.extensions.sensors import CurwWrfFileLockSensor
from curw.workflow.airflow.extensions import tasks
from curw.workflow.airflow.dags import utils as dag_utils
from curw.rainfall.wrf import utils


def get_gfs_download_subdag(parent_dag_name, child_dag_name, args, wrf_config_key='wrf_config', test_mode=False):
    dag_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None,
    )

    try:
        wrf_config = WrfConfig(configs=Variable.get(wrf_config_key, deserialize_json=True))
    except KeyError as e:
        logging.error('Key error %s' % str(e))
        return dag_subdag

    period = wrf_config.get('period')
    step = wrf_config.get('gfs_step')
    gfs_dir = wrf_config.get('gfs_dir')
    try:
        gfs_date, gfs_cycle, start = utils.get_appropriate_gfs_inventory(wrf_config)
        logging.info('Gfs date %s, gfs cycle %s, start inventory %s' % (gfs_date, gfs_cycle, str(start).zfill(3)))
    except KeyError as e:
        # raise WrfRunException(str(e))
        logging.error('Unable to find the key: %s. Returining an empty subdag' % str(e))
        return dag_subdag

    gfs_clean = PythonOperator(
        python_callable=gfs_cleanup,
        task_id='%s-task-%s' % (child_dag_name, 'gfs_cleanup'),
        op_args=[wrf_config.get('gfs_clean'), gfs_dir],
        default_args=args,
        dag=dag_subdag,
    )

    for i in range(0, period * 24 + 1, step):
        logging.info('Adding %dth inventory as %d' % (int(start) + i, i))
        t = PythonOperator(
            python_callable=download_inventory.download_i_th_inventory,
            task_id='%s-task-%s' % (child_dag_name, i),
            op_args=[int(start) + i, wrf_config.get('gfs_url'), wrf_config.get('gfs_inv'), gfs_date, gfs_cycle,
                     wrf_config.get('gfs_res'), gfs_dir, wrf_config.get('nfs_dir'), test_mode],
            # provide_context=True,
            default_args=args,
            dag=dag_subdag,
        )
        gfs_clean >> t

    return dag_subdag


def get_initial_parameters_subdag(parent_dag_name, child_dag_name, runs, args, wrf_home_key, wrf_start_date_key,
                                  wrf_config_key):
    dag_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None,
    )

    for i in [str(x) for x in range(runs)]:
        PythonOperator(
            task_id='%s-task-%s' % (child_dag_name, i),
            python_callable=dag_utils.set_initial_parameters_fs,
            provide_context=True,
            op_args=[wrf_home_key + i, wrf_start_date_key + i, wrf_config_key + i],
            default_args=args,
            dag=dag_subdag,
        )

    return dag_subdag


def get_wrf_run_subdag(parent_dag_name, child_dag_name, runs, args, wrf_config_key, test_mode=False):
    dag_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval=None,
    )

    for i in [str(x) for x in range(runs)]:
        lock_sensor = CurwWrfFileLockSensor(
            config_key=wrf_config_key + i,
            task_id='%s-task-%s-%s' % (child_dag_name, 'lock_sensor', i),
            poke_interval=60,  # poke every minute
            timeout=60 * 60 * 24,  # timeout after a day
            default_args=args,
            dag=dag_subdag
        )

        acquire_lock = PythonOperator(
            task_id='%s-task-%s-%s' % (child_dag_name, 'acquire_lock', i),
            python_callable=acquire_wrf_lock,
            op_args=[wrf_config_key + i],
            default_args=args,
            dag=dag_subdag
        )

        real = CurwPythonOperator(
            task_id='%s-task-%s-%s' % (child_dag_name, 'real', i),
            curw_task=tasks.Real,
            init_args=[wrf_config_key + i],
            provide_context=True,
            default_args=args,
            dag=dag_subdag,
            test_mode=test_mode
        )

        wrf = CurwPythonOperator(
            task_id='%s-task-%s-%s' % (child_dag_name, 'wrf', i),
            curw_task=tasks.Wrf,
            init_args=[wrf_config_key + i],
            provide_context=True,
            default_args=args,
            dag=dag_subdag,
            test_mode=test_mode
        )

        release_lock = PythonOperator(
            task_id='%s-task-%s-%s' % (child_dag_name, 'release_lock', i),
            python_callable=release_wrf_lock,
            op_args=[wrf_config_key + i],
            default_args=args,
            dag=dag_subdag
        )

        release_lock_if_failed = PythonOperator(
            task_id='%s-task-%s-%s' % (child_dag_name, 'release_lock_failed', i),
            python_callable=release_wrf_lock,
            op_args=[wrf_config_key + i],
            default_args=args,
            dag=dag_subdag,
            trigger_rule=TriggerRule.ONE_FAILED
        )

        args['retries'] = 3
        rf_extraction = CurwPythonOperator(
            task_id='%s-task-%s-%s' % (child_dag_name, 'extraction', i),
            curw_task=tasks.RainfallExtraction,
            init_args=[wrf_config_key + i],
            provide_context=True,
            default_args=args,
            dag=dag_subdag,
            test_mode=test_mode
        )

        rf_extraction2 = CurwPythonOperator(
            task_id='%s-task-%s-%s' % (child_dag_name, 'extraction-D01', i),
            curw_task=tasks.RainfallExtractionD01,
            init_args=[wrf_config_key + i],
            provide_context=True,
            default_args=args,
            dag=dag_subdag,
            test_mode=test_mode
        )

        lock_sensor >> acquire_lock >> real >> wrf >> release_lock >> [rf_extraction, rf_extraction2]
        wrf >> release_lock_if_failed

    return dag_subdag


def acquire_wrf_lock(wrf_config_key):
    config = WrfConfig(Variable.get(wrf_config_key, deserialize_json=True))
    file_path = os.path.join(utils.get_em_real_dir(config.get('wrf_home')), 'wrf.lock')
    logging.info('acquiring lock %s' % file_path)
    with open(file_path, 'w') as lock:
        lock.write(config.to_string())


def release_wrf_lock(wrf_config_key):
    config = WrfConfig(Variable.get(wrf_config_key, deserialize_json=True))
    file_path = os.path.join(utils.get_em_real_dir(config.get('wrf_home')), 'wrf.lock')
    logging.info('releasing lock %s' % file_path)
    os.remove(file_path)


def gfs_cleanup(gfs_clean, gfs_dir):
    if bool(gfs_clean):
        logging.info('Cleaning the GFS dir: %s' % gfs_dir)
        try:
            utils.cleanup_dir(gfs_dir)
        except (OSError, PermissionError) as e:
            logging.error('Unable ro cleanup dir %s : %s' % (gfs_dir, str(e)))
