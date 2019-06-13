import datetime as dt

import airflow
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from curw.workflow.airflow.extensions import subdags
from curw.workflow.airflow.extensions import tasks
from curw.workflow.airflow.extensions.operators.curw_python_operator import CurwPythonOperator

wrf_dag_name = 'wrf_parallel_run_v4'
wrf_config_key_prefix = 'wrf_config'
wrf_home_key_prefix = 'wrf_home'
wrf_start_date_key_prefix = 'wrf_start_date'
parallel_runs = 5
wt_namelists = 'wt_namelists'
wt_namelists_path = '/mnt/disks/wrf-mod/namelist/'
queue = 'wrf_fs_impl_queue'
schedule_interval = '0 18 * * *'

test_mode = False

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
    description='Running 4 WRFs simultaneously',
    schedule_interval=schedule_interval)

initialize_params = SubDagOperator(
    task_id='initialize_params',
    subdag=subdags.get_initial_parameters_subdag(wrf_dag_name, 'initialize_params', parallel_runs, default_args,
                                                 wrf_home_key_prefix, wrf_start_date_key_prefix,
                                                 wrf_config_key_prefix),
    default_args=default_args,
    dag=dag,
)

gfs_data_download = SubDagOperator(
    task_id='gfs_download',
    subdag=subdags.get_gfs_download_subdag(wrf_dag_name, 'gfs_download', default_args,
                                           wrf_config_key=wrf_config_key_prefix + '0', test_mode=test_mode),
    default_args=default_args,
    dag=dag,
)

ungrib = CurwPythonOperator(
    task_id='ungrib',
    curw_task=tasks.Ungrib,
    init_args=[wrf_config_key_prefix + '0'],
    provide_context=True,
    default_args=default_args,
    dag=dag,
    test_mode=test_mode
)

find_weather_type = CurwPythonOperator(
    task_id='find_weather_type',
    curw_task=tasks.FindWeatherType,
    init_args=[[wrf_config_key_prefix + str(x) for x in range(parallel_runs)], wt_namelists, wt_namelists_path],
    provide_context=True,
    default_args=default_args,
    dag=dag,
    test_mode=test_mode
)

geogrid = CurwPythonOperator(
    task_id='geogrid',
    curw_task=tasks.Geogrid,
    init_args=[wrf_config_key_prefix + '0'],
    provide_context=True,
    default_args=default_args,
    dag=dag,
    test_mode=test_mode
)

metgrid = CurwPythonOperator(
    task_id='metgrid',
    curw_task=tasks.Metgrid,
    init_args=[wrf_config_key_prefix + '0'],
    provide_context=True,
    default_args=default_args,
    dag=dag,
    test_mode=test_mode
)

real_wrf = SubDagOperator(
    task_id='real_wrf',
    subdag=subdags.get_wrf_run_subdag(wrf_dag_name, 'real_wrf', parallel_runs, default_args, wrf_config_key_prefix,
                                      test_mode=test_mode),
    default_args=default_args,
    dag=dag,
)

initialize_params >> [gfs_data_download, geogrid, ungrib]

gfs_data_download >> ungrib

gfs_data_download >> find_weather_type

metgrid << [geogrid, ungrib]

real_wrf << [find_weather_type, metgrid]
