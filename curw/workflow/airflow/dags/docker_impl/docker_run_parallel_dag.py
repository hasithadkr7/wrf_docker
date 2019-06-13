import datetime as dt

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from curw.workflow.airflow.dags import utils as dag_utils
from curw.workflow.airflow.extensions.operators.curw_docker_operator import CurwDockerOperator

wrf_dag_name = 'docker_wrf_run_v3'
wrf_config_key = 'docker_wrf_config'
namelist_wps_key = 'docker_namelist_wps'
namelist_input_key = 'docker_namelist_input'
wrf_home_key = 'docker_wrf_home'
wrf_start_date_key = 'docker_wrf_start_date'
# queue = 'wrf_docker_queue'
queue = 'default'
schedule_interval = '0 18 * * *'

run_id = '{{ var.json.%s.start_date }}_test' % wrf_config_key

image = 'nirandaperera/curw-wrf-391'
volumes = ['/mnt/disks/wrf-mod/temp1/output:/wrf/output', '/mnt/disks/wrf-mod/DATA/geog:/wrf/geog']

# git_path = '/opt/git/models'

test_mode = False


def get_docker_cmd(run_id, wrf_config, mode, nl_wps, nl_input):
    cmd = '/wrf/run_wrf.sh -i \"%s\" -c \"%s\" -m \"%s\" -x \"%s\" -y \"%s\"' % (
        run_id, wrf_config, mode, nl_wps, nl_input)
    return cmd


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
}

# initiate the DAG
dag = DAG(
    wrf_dag_name,
    default_args=default_args,
    description='Running 4 WRFs simultaneously using docker',
    schedule_interval=schedule_interval)

initialize_params = PythonOperator(
    task_id='init-params',
    python_callable=dag_utils.set_initial_parameters_fs,
    provide_context=True,
    op_args=[wrf_home_key, wrf_start_date_key, wrf_config_key, True],
    default_args=default_args,
    dag=dag,
)

wps = CurwDockerOperator(
    task_id='wps',
    image=image,
    command=get_docker_cmd(run_id, '{{ var.json.%s }}' % wrf_config_key, 'wps', '{{ var.value.%s }}' % namelist_wps_key,
                           '{{ var.value.%s }}' % namelist_input_key),
    cpus=1,
    volumes=volumes,
    auto_remove=True,
    dag=dag
)

wrf = CurwDockerOperator(
    task_id='wrf',
    image=image,
    command=get_docker_cmd(run_id, '{{ var.json.%s }}' % wrf_config_key, 'wrf', '{{ var.value.%s }}' % namelist_wps_key,
                           '{{ var.value.%s }}' % namelist_input_key),
    cpus=2,
    volumes=volumes,
    auto_remove=True,
    dag=dag
)

initialize_params >> wps >> wrf
