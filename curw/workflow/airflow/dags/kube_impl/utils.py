import base64
import json
import logging

import os

from curw.rainfall.wrf import utils
from curw.container.docker.rainfall import utils as docker_rf_utils
from curw.workflow.airflow.dags.docker_impl import utils as airflow_docker_utils


def get_resource_name_jinja_template(resource):
    return '-'.join([resource.lower(), '{{ti.dag_id}}', '{{ti.task_id}}', '{{ti.job_id}}'])


def initialize_config(config_str, configs_prefix='wrf_config_', **context):
    config = json.loads(config_str)

    for k, v in context['templates_dict'].items():
        if k.startswith(configs_prefix):
            config[k.replace(configs_prefix, '')] = v

    config_str = json.dumps(config, sort_keys=True)
    logging.info('Initialized wrf_config:\n' + config_str)

    b64_config = docker_rf_utils.get_base64_encoded_str(config_str)
    logging.info('Returned xcom:\n' + b64_config)
    return b64_config


def generate_random_run_id(prefix, random_str_len=4, suffix=None, **context):
    run_id = '_'.join(
        [prefix, airflow_docker_utils.get_start_date_from_context(context).strftime('%Y-%m-%d_%H:%M'),
         suffix if suffix else airflow_docker_utils.id_generator(size=random_str_len)])
    logging.info('Generated run_id: ' + run_id)
    return run_id


def suffix_run_id(**context):
    # splits = context['templates_dict']['run_id'].split('_')
    # run_id = '_'.join([splits[0] + context['templates_dict']['run_id_suffix']] + splits[1:])
    run_id = context['templates_dict']['run_id'] + '_' + context['templates_dict']['run_id_suffix']
    logging.info('Suffixed run_id: ' + run_id)
    return run_id


def clean_up_wrf_run(init_task_id, **context):
    config_str = docker_rf_utils.get_base64_decoded_str(context['task_instance'].xcom_pull(task_ids=init_task_id))
    wrf_config = json.loads(config_str)

    metgrid_path = os.path.join(wrf_config.get('nfs_dir'), 'metgrid', wrf_config.get('run_id') + '_metgrid.zip')
    if utils.file_exists_nonempty(metgrid_path):
        logging.info('Cleaning up metgrid ' + metgrid_path)
        os.remove(metgrid_path)
    else:
        logging.info('No metgrid file')


def check_data_push_callable(task_ids, **context):
    exec_date = airflow_docker_utils.get_start_date_from_context(context)
    if exec_date.hour == 18 and exec_date.minute == 0:
        return task_ids[0]
    else:
        return task_ids[1]
