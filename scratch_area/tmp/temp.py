# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](http://pythonhosted.org/airflow/tutorial.html)
"""
import datetime as dt
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from curw.rainfall.wrf.execution import executor as wrf_exec

# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization



default_args = {
    'owner': 'curw_admin',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['admin@curwsl.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

dag = DAG(
    'temp',
    default_args=default_args,
    description='Running WRF',
    schedule_interval=timedelta(days=1))

wrf_config = wrf_exec.get_wrf_config('/tmp')
start = dt.datetime.now()
end = start + dt.timedelta(1)


def temp_def(a, b, **kwargs):
    print('{% {{ds}} %}')
    print('{{execution_date}}')
    print('a=%s, b=%s, kwargs=%s' % (str(a), str(b), str(kwargs)))


ds = '{{ ds }}'
mm = '{{ macros.datetime.now() }}'
# t1 = PythonOperator(
#     task_id='download_gfs_data',
#     python_callable=temp_def,
#     op_args=[mm , ds],
#     provide_context=False,
#     # templates_dict={'aa': 'aaaaaa'},
#     dag=dag)

print(str(Variable.get('test_key')))

Variable.set('aaa', 'fff')
Variable.set('test_key', 'fff')

t1 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command='echo %s %s %s' % (ds, mm, '{{ execution_date }}'),
    params={'my_param': 'Parameter I passed in'},
    dag=dag)

t1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets    
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

dag.doc_md = __doc__
