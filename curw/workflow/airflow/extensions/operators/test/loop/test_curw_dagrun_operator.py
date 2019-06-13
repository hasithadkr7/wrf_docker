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
"""This example illustrates the use of the TriggerDagRunOperator. There are 2
entities at work in this scenario:
1. The Controller DAG - the DAG that conditionally executes the trigger
2. The Target DAG - DAG being triggered (in example_trigger_target_dag.py)

This example illustrates the following features :
1. A TriggerDagRunOperator that takes:
  a. A python callable that decides whether or not to trigger the Target DAG
  b. An optional params dict passed to the python callable to help in
     evaluating whether or not to trigger the Target DAG
  c. The id (name) of the Target DAG
  d. The python callable can add contextual info to the DagRun created by
     way of adding a Pickleable payload (e.g. dictionary of primitives). This
     state is then made available to the TargetDag
2. A Target DAG : c.f. example_trigger_target_dag.py
"""

import pprint
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from curw.workflow.airflow.extensions.operators.curw_dag_loop_operator import CurwDagLoopOperator

pp = pprint.PrettyPrinter(indent=4)


def conditionally_trigger(context, dag_run_obj):
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['condition_param']:
        dag_run_obj.payload = {'message': context['params']['message']}
        pp.pprint(dag_run_obj.payload)
        return dag_run_obj


# Define the DAG
dag = DAG(dag_id='loop_dag',
          default_args={"owner": "airflow",
                        "start_date": datetime.utcnow()},
          schedule_interval='@once')

# Define the single task in this controller example DAG
trigger = CurwDagLoopOperator(task_id='trigger',
                              # trigger_dag_id="example_trigger_target_dag",
                              # loop_id='test1',
                              python_callable=conditionally_trigger,
                              params={'condition_param': True,
                                            'message': 'Hello World'},
                              dag=dag)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 30',
    dag=dag)

t2 >> trigger >> t1