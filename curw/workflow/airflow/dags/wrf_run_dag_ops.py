import datetime as dt

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from curw.rainfall.wrf.execution import tasks
from curw.workflow.airflow.dags import utils as dag_utils
from curw.workflow.airflow.extensions import subdags
from curw.workflow.airflow.extensions.operators import CurwPythonOperator

wrf_dag_name = 'wrf_run_ops'
wrf_home_key = 'wrf_home'
wrf_start_date_key = 'wrf_start_date'
wrf_config_key = 'wrf_config'
queue = 'default'  # 'wrf_fs_impl_queue'

default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
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
    description='Running WRF instantaneously',
    schedule_interval=None)

initialize_params = PythonOperator(
    task_id='initialize-params',
    python_callable=dag_utils.set_initial_parameters_fs,
    provide_context=True,
    op_args=[wrf_home_key, wrf_start_date_key, wrf_config_key],
    default_args=default_args,
    dag=dag,
)

gfs_data_download = SubDagOperator(
    task_id='gfs_download',
    subdag=subdags.get_gfs_download_subdag(wrf_dag_name, 'gfs_download', default_args),
    default_args=default_args,
    dag=dag,
)

ungrib = CurwPythonOperator(
    task_id='ungrib',
    curw_task=tasks.Ungrib,
    provide_context=True,
    default_args=default_args,
    dag=dag,
)

geogrid = CurwPythonOperator(
    task_id='geogrid',
    curw_task=tasks.Geogrid,
    provide_context=True,
    default_args=default_args,
    dag=dag,
)

metgrid = CurwPythonOperator(
    task_id='metgrid',
    curw_task=tasks.Metgrid,
    provide_context=True,
    default_args=default_args,
    dag=dag,
)

real = CurwPythonOperator(
    task_id='real',
    curw_task=tasks.Real,
    provide_context=True,
    default_args=default_args,
    dag=dag,
)

wrf = CurwPythonOperator(
    task_id='wrf',
    curw_task=tasks.Wrf,
    provide_context=True,
    default_args=default_args,
    dag=dag,
)

initialize_params >> [gfs_data_download, geogrid, ungrib]

gfs_data_download >> ungrib

metgrid << [geogrid, ungrib]

metgrid >> real >> wrf
