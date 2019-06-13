import airflow
import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

sat_dag_name = 'sat_rf_extraction_v4'
# queue = 'wrf_fs_impl_queue'
schedule_interval = '30 * * * *'
curw_py_dir = '/opt/git/models'
output_dir = '/mnt/disks/curwsl_nfs/sat'
# curw_py_dir = '/home/curw/git/models'
# output_dir = '/home/curw/temp'

default_args = {
    'owner': 'curwsl admin',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['admin@curwsl.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 6,
    'retry_delay': dt.timedelta(minutes=15),
    # 'queue': queue,
}

# initiate the DAG
dag = DAG(
    sat_dag_name,
    default_args=default_args,
    description='Extracting JAXA satellite rainfall data hourly',
    schedule_interval=schedule_interval)

cmd_str_d03 = 'python3 %s/curw/rainfall/wrf/extraction/sat_extractor.py ' \
              '-end {{ macros.datetime.strftime(execution_date + macros.timedelta(hours=1), \'%%Y-%%m-%%d_%%H:%%M\') }} ' \
              '-start {{ execution_date.strftime(\'%%Y-%%m-%%d_%%H:%%M\') }} ' \
              '-output %s' % (curw_py_dir, output_dir)

# D01 boundaries
lat_min = max(5.0, -3.06107)
lon_min = max(60.0, 71.2166)
lat_max = min(40.0, 18.1895)
lon_max = min(93.0, 90.3315)

cmd_str_d01 = 'python3 %s/curw/rainfall/wrf/extraction/sat_extractor.py ' \
              '-end {{ macros.datetime.strftime(execution_date + macros.timedelta(hours=1), \'%%Y-%%m-%%d_%%H:%%M\') }} ' \
              '-start {{ execution_date.strftime(\'%%Y-%%m-%%d_%%H:%%M\') }} ' \
              '-output %s -prefix %s -lat_min %f -lon_min %f -lat_max %f -lon_max %f' % (
              curw_py_dir, output_dir, 'jaxa_sat_D01', lat_min, lon_min, lat_max, lon_max)

BashOperator(
    task_id='sat_rf_extraction_D03',
    bash_command=cmd_str_d03,
    default_args=default_args,
    dag=dag
)

BashOperator(
    task_id='sat_rf_extraction_D01',
    bash_command=cmd_str_d01,
    default_args=default_args,
    dag=dag
)
