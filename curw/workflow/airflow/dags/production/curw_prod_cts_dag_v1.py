import datetime as dt
import json
import logging
import os

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from curw.container.docker.rainfall import utils as docker_rf_utils
from curw.rainfall.wrf import utils
from curw.workflow.airflow.dags.docker_impl import utils as airflow_docker_utils
from curw.workflow.airflow.extensions.operators.curw_docker_operator import CurwDockerOperator

prod_dag_name = 'curw_prod_cts_dag_v1'
queue = 'default'
schedule_interval = '0 18 * * *'
dag_pool = 'curw_prod_runs'

parallel_runs = 1
priorities = [1000, 10, 10, 10, 10, 10]

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

wrf_config_key = 'docker_wrf_config_prod_cts'
wrf_run_id_prefix = 'wrf0'

airflow_vars = airflow_docker_utils.check_airflow_variables(
    nl_inp_keys + [nl_wps_key, curw_db_config_path, wrf_config_key],
    ignore_error=True)

default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['admin@curwsl.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
    'queue': queue,
    'catchup': False,
}

# initiate the DAG
dag = DAG(
    prod_dag_name,
    default_args=default_args,
    description='Curw production DAG',
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


def generate_random_run_id(prefix, random_str_len=4, suffix=None, **context):
    run_id = '_'.join(
        [prefix, airflow_docker_utils.get_start_date_from_context(context).strftime('%Y-%m-%d_%H:%M'),
         suffix if suffix else airflow_docker_utils.id_generator(size=random_str_len)])
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


# ---------------- WRF ----------------

generate_run_id = PythonOperator(
    task_id='gen-run-id',
    python_callable=generate_random_run_id,
    op_args=[wrf_run_id_prefix],
    op_kwargs={"suffix": "0000"},
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
    pool=dag_pool,
    retries=3,
    retry_delay=dt.timedelta(minutes=10),
)

generate_run_id >> init_config >> wps

select_wrf = DummyOperator(task_id='select-wrf', dag=dag)

i = 0

wrf = CurwDockerOperator(
    task_id='wrf%d' % i,
    image=wrf_image,
    docker_url=docker_url,
    command=airflow_docker_utils.get_docker_cmd('{{ task_instance.xcom_pull(task_ids=\'gen-run-id\') }}',
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
    pool=dag_pool,
    priority_weight=priorities[i]
)

extract_wrf = CurwDockerOperator(
    task_id='wrf%d-extract' % i,
    image=extract_image,
    docker_url=docker_url,
    command=airflow_docker_utils.get_docker_extract_cmd(
        '{{ task_instance.xcom_pull(task_ids=\'gen-run-id\') }}',
        '{{ task_instance.xcom_pull(task_ids=\'init-config\') }}',
        docker_rf_utils.get_base64_encoded_str(airflow_vars[curw_db_config_path]),
        gcs_volumes,
        overwrite=False),
    cpus=4,
    volumes=docker_volumes,
    auto_remove=True,
    privileged=True,
    dag=dag,
    pool=dag_pool,
    priority_weight=priorities[i]
)

wps >> wrf >> extract_wrf >> select_wrf >> clean_up

# ---------------- HEC-HMS & FLO2D----------------
run_hec_flo2d_cmd = "ssh -i /home/uwcc-admin/.ssh/uwcc-admin -o \"StrictHostKeyChecking no\" uwcc-admin@10.138.0.3 " \
                    "\'bash -c \"/home/uwcc-admin/udp/Forecast.sh -f -B 2 " \
                    "-d {{ macros.datetime.strftime(execution_date + macros.timedelta(days=1), \'%Y-%m-%d\') }}\" \'"
run_hec_flo2d = BashOperator(
    task_id='run-hec-flo2d-local',
    bash_command=run_hec_flo2d_cmd,
    dag=dag,
    pool=dag_pool,
    priority_weight=priorities[i]
)

wait_for_30m = BashOperator(
    task_id='wait-for-30m',
    bash_command='sleep 30m',
    dag=dag,
    pool=dag_pool,
    priority_weight=priorities[i]
)

extract_wl_cmd = "ssh -i /home/uwcc-admin/.ssh/uwcc-admin -o \"StrictHostKeyChecking no\" " \
                 "uwcc-admin@10.138.0.3 'bash -c \"/home/uwcc-admin/udp/Trigger_Extract_WaterLevel.sh " \
                 "-d {{ macros.datetime.strftime(execution_date + macros.timedelta(days=1), \'%Y-%m-%d\') }}\" \'"
extract_water_levels = BashOperator(
    task_id='extract-water-levels',
    bash_command=extract_wl_cmd,
    dag=dag,
    pool=dag_pool,
    priority_weight=priorities[i]
)

clean_up >> run_hec_flo2d >> wait_for_30m >> extract_water_levels
