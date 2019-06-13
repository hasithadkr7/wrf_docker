import datetime as dt
import json
import logging
import os

import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from curw.rainfall.wrf import utils
from curw.container.docker.rainfall import utils as docker_rf_utils
from curw.workflow.airflow.dags.docker_impl import utils as airflow_docker_utils
from curw.workflow.airflow.extensions.operators.curw_docker_operator import CurwDockerOperator

wrf_dag_name = 'docker_wrf_run_prod_inst_v4_6d'
queue = 'docker_prod_queue'
schedule_interval = '0 18 * * *'

parallel_runs = 6
# priorities = [1000, 1, 1, 1, 1, 1]
priorities = [1, -1, 1, -1, -1, -1]

docker_url = 'tcp://10.128.0.5:2375'

# docker images
wrf_image = 'nirandaperera/curw-wrf-391'
extract_image = 'nirandaperera/curw-wrf-391-extract'

# volumes and mounts
curw_nfs = 'curwsl_nfs_1'
curw_archive = 'curwsl_archive_1'
local_geog_dir = "/mnt/disks/workspace1/wrf-data/geog"
local_gcs_config_path = '/home/uwcc-admin/uwcc-admin.json'
docker_volumes = ['%s:/wrf/geog' % local_geog_dir, '%s:/wrf/gcs.json' % local_gcs_config_path]
gcs_volumes = ['%s:/wrf/output' % curw_nfs, '%s:/wrf/archive' % curw_archive]

# namelist keys
nl_wps_key = "nl_wps"
nl_inp_keys = ["nl_inp_SIDAT", "nl_inp_C", "nl_inp_H", "nl_inp_NW", "nl_inp_SW", "nl_inp_W"]

curw_db_config_path = 'curw_db_config'

wrf_config_key = 'docker_wrf_config_prod_6d'

wrf_pool = 'parallel_wrf_runs'
run_id_prefix = 'wrf-doc-prod-6d'

airflow_vars = airflow_docker_utils.check_airflow_variables(
    nl_inp_keys + [nl_wps_key, curw_db_config_path, wrf_config_key],
    ignore_error=True)

default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    # 'start_date': airflow.utils.dates.days_ago(1),
    'start_date': dt.datetime(2018, 1, 23, 18, 00),
    'email': ['admin@curwsl.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
    'queue': queue,
    'end_date': dt.datetime(2018, 2, 3, 18, 00),
}

# initiate the DAG
dag = DAG(
    wrf_dag_name,
    default_args=default_args,
    description='Running 6 WRFs simultaneously using docker - 6day forecast',
    schedule_interval=schedule_interval)


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


def generate_random_run_id(prefix, random_str_len=4, **context):
    run_id = '_'.join(
        [prefix, airflow_docker_utils.get_start_date_from_context(context).strftime('%Y-%m-%d_%H:%M'),
         airflow_docker_utils.id_generator(size=random_str_len)])
    logging.info('Generated run_id: ' + run_id)
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


generate_run_id = PythonOperator(
    task_id='gen-run-id',
    python_callable=generate_random_run_id,
    op_args=[run_id_prefix],
    provide_context=True,
    dag=dag
)

init_config = PythonOperator(
    task_id='init-config',
    python_callable=initialize_config,
    provide_context=True,
    op_args=[airflow_vars[wrf_config_key], 'wrf_config_'],
    templates_dict={
        'wrf_config_start_date': airflow_docker_utils.get_start_date_template(),
        'wrf_config_run_id': '{{ task_instance.xcom_pull(task_ids=\'gen-run-id\') }}',
        'wrf_config_wps_run_id': '{{ task_instance.xcom_pull(task_ids=\'gen-run-id\') }}',
    },
    dag=dag,
)

clean_up = PythonOperator(
    task_id='cleanup-config',
    python_callable=clean_up_wrf_run,
    provide_context=True,
    op_args=['init-config'],
    dag=dag,
)

wps = CurwDockerOperator(
    task_id='wps',
    image=wrf_image,
    docker_url=docker_url,
    command=airflow_docker_utils.get_docker_cmd('{{ task_instance.xcom_pull(task_ids=\'gen-run-id\') }}',
                                                '{{ task_instance.xcom_pull(task_ids=\'init-config\') }}',
                                                'wps',
                                                docker_rf_utils.get_base64_encoded_str(airflow_vars[nl_wps_key]),
                                                docker_rf_utils.get_base64_encoded_str(airflow_vars[nl_inp_keys[0]]),
                                                gcs_volumes),
    cpus=1,
    volumes=docker_volumes,
    auto_remove=True,
    privileged=True,
    dag=dag,
    pool=wrf_pool,
)

generate_run_id >> init_config >> wps

select_wrf = DummyOperator(task_id='select-wrf', dag=dag)

for i in range(parallel_runs):
    if priorities[i] < 0:
        continue

    generate_run_id_wrf = PythonOperator(
        task_id='gen-run-id-wrf%d' % i,
        python_callable=generate_random_run_id,
        op_args=[run_id_prefix + str(i)],
        provide_context=True,
        dag=dag,
        priority_weight=priorities[i]
    )

    wrf = CurwDockerOperator(
        task_id='wrf%d' % i,
        image=wrf_image,
        docker_url=docker_url,
        command=airflow_docker_utils.get_docker_cmd('{{ task_instance.xcom_pull(task_ids=\'gen-run-id-wrf%d\') }}' % i,
                                                    '{{ task_instance.xcom_pull(task_ids=\'init-config\') }}',
                                                    'wrf',
                                                    docker_rf_utils.get_base64_encoded_str(airflow_vars[nl_wps_key]),
                                                    docker_rf_utils.get_base64_encoded_str(
                                                        airflow_vars[nl_inp_keys[i]]),
                                                    gcs_volumes),
        cpus=4,
        volumes=docker_volumes,
        auto_remove=True,
        privileged=True,
        dag=dag,
        pool=wrf_pool,
        priority_weight=priorities[i]
    )

    extract_wrf = CurwDockerOperator(
        task_id='wrf%d-extract' % i,
        image=extract_image,
        docker_url=docker_url,
        command=airflow_docker_utils.get_docker_extract_cmd(
            '{{ task_instance.xcom_pull(task_ids=\'gen-run-id-wrf%d\') }}' % i,
            '{{ task_instance.xcom_pull(task_ids=\'init-config\') }}',
            docker_rf_utils.get_base64_encoded_str(airflow_vars[curw_db_config_path]),
            gcs_volumes,
            overwrite=False),
        cpus=4,
        volumes=docker_volumes,
        auto_remove=True,
        privileged=True,
        dag=dag,
        pool=wrf_pool,
        priority_weight=priorities[i]
    )

    extract_wrf_no_data_push = CurwDockerOperator(
        task_id='wrf%d-extract-no-data-push' % i,
        image=extract_image,
        docker_url=docker_url,
        command=airflow_docker_utils.get_docker_extract_cmd(
            '{{ task_instance.xcom_pull(task_ids=\'gen-run-id-wrf%d\') }}' % i,
            '{{ task_instance.xcom_pull(task_ids=\'init-config\') }}',
            docker_rf_utils.get_base64_encoded_str('{}'),
            gcs_volumes,
            overwrite=False),
        cpus=4,
        volumes=docker_volumes,
        auto_remove=True,
        privileged=True,
        dag=dag,
        pool=wrf_pool,
        priority_weight=priorities[i]
    )

    check_data_push = BranchPythonOperator(
        task_id='check-data-push' + str(i),
        python_callable=check_data_push_callable,
        provide_context=True,
        op_args=[['wrf%d-extract' % i, 'wrf%d-extract-no-data-push' % i]],
        dag=dag,
        priority_weight=priorities[i]
    )

    join_branch = DummyOperator(
        task_id='join-branch' + str(i),
        trigger_rule='one_success',
        dag=dag,
        priority_weight=priorities[i]
    )

    wps >> generate_run_id_wrf >> wrf >> check_data_push
    check_data_push >> extract_wrf >> join_branch
    check_data_push >> extract_wrf_no_data_push >> join_branch
    join_branch >> select_wrf

select_wrf >> clean_up
