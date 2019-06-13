import datetime as dt
import logging
import os

from kubernetes import client

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from curw.workflow.airflow import utils as af_utils
from curw.workflow.airflow.dags.docker_impl import utils as af_docker_utils
from curw.workflow.airflow.dags.kube_impl import utils as af_kube_utils
from curw.workflow.airflow.extensions.operators.curw_gke_operator_v2 import CurwGkeOperatorV2

dag_name = 'kube_wrf_prod_cts_v1'
queue = 'docker_prod_queue'  # reusing docker queue 
schedule_interval = '0 18 * * *'

parallel_runs = 6
priorities = [1000, 1, 1, 1, 1, 1]

run_id_prefix = 'kube-wrf-prod-cts'

# aiflow variable keys
curw_db_config = Variable.get('curw_db_config')
wrf_config = Variable.get('kube_wrf_config_cts')
nl_wps = Variable.get('nl_wps')
nl_inputs = [Variable.get(k) for k in ["nl_inp_SIDAT", "nl_inp_C", "nl_inp_H", "nl_inp_NW", "nl_inp_SW", "nl_inp_W"]]

# kube images
wrf_image = 'us.gcr.io/uwcc-160712/curw-wrf-391'
extract_image = 'us.gcr.io/uwcc-160712/curw-wrf-391-extract'

# volumes and mounts
vols = [client.V1Volume(name='geog-vol',
                        gce_persistent_disk=client.V1GCEPersistentDiskVolumeSource(pd_name='wrf-geog-disk1',
                                                                                   read_only=True)),
        client.V1Volume(name='sec-vol', secret=client.V1SecretVolumeSource(secret_name='google-app-creds'))]

vol_mounts = [client.V1VolumeMount(mount_path='/wrf/geog', name='geog-vol'),
              client.V1VolumeMount(mount_path='/wrf/config', name='sec-vol')]

# secrets
secrets = [client.V1Secret(kind='Secret',
                           type='Opaque',
                           metadata=client.V1ObjectMeta(name='google-app-creds'),
                           data={'gcs.json': af_utils.get_base64_encoded_str(
                               af_utils.read_file(os.getenv('GOOGLE_APPLICATION_CREDENTIALS')))})]


def get_base_pod():
    return client.V1Pod(
        metadata=client.V1ObjectMeta(),
        spec=client.V1PodSpec(
            containers=[
                client.V1Container(
                    name='name',
                    volume_mounts=vol_mounts,
                    security_context=client.V1SecurityContext(privileged=True),
                    image_pull_policy='IfNotPresent'
                )],
            restart_policy='OnFailure',
            volumes=vols
        )
    )


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
    dag_name,
    default_args=default_args,
    description='Running multiple WRFs using Google Kubernetes Engine',
    schedule_interval=schedule_interval)

generate_run_id = PythonOperator(
    task_id='gen-run-id',
    python_callable=af_kube_utils.generate_random_run_id,
    op_args=[run_id_prefix],
    op_kwargs={"suffix": "0000"},
    provide_context=True,
    dag=dag
)

init_config = PythonOperator(
    task_id='init-config',
    python_callable=af_kube_utils.initialize_config,
    provide_context=True,
    op_args=[wrf_config, 'wrf_config_'],
    templates_dict={
        'wrf_config_start_date': af_docker_utils.get_start_date_template(0),
        # ** NOTE: this should change for a scheduled run
        'wrf_config_run_id': '{{ task_instance.xcom_pull(task_ids=\'gen-run-id\') }}',
        'wrf_config_wps_run_id': '{{ task_instance.xcom_pull(task_ids=\'gen-run-id\') }}',
    },
    dag=dag,
)

clean_up = PythonOperator(
    task_id='cleanup-config',
    python_callable=af_kube_utils.clean_up_wrf_run,
    provide_context=True,
    op_args=['init-config'],
    dag=dag,
)

logging.info('Initializing wps pod')
wps_pod = get_base_pod()
wps_pod.metadata.name = 'wps-pod-{{ ti.xcom_pull(task_ids=\'gen-run-id\') }}'
wps_pod.spec.containers[0].name = 'wps-cont-{{ ti.xcom_pull(task_ids=\'gen-run-id\') }}'
wps_pod.spec.containers[0].image = wrf_image
wps_pod.spec.containers[0].command = ['/wrf/run_wrf.sh']
wps_pod.spec.containers[0].resources = client.V1ResourceRequirements(requests={'cpu': 1, 'memory': '6G'})
wps_pod.spec.containers[0].args = ['-i', '{{ ti.xcom_pull(task_ids=\'gen-run-id\') }}',
                                   '-c', '{{ ti.xcom_pull(task_ids=\'init-config\') }}',
                                   '-m', 'wps',
                                   '-x', '%s' % af_utils.get_base64_encoded_str(nl_wps),
                                   '-y', '%s' % af_utils.get_base64_encoded_str(nl_inputs[0]),
                                   '-k', '/wrf/config/gcs.json',
                                   '-v', 'curwsl_nfs_1:/wrf/output',
                                   '-v', 'curwsl_archive_1:/wrf/archive',
                                   ]

wps = CurwGkeOperatorV2(
    task_id='wps',
    pod=wps_pod,
    secret_list=secrets or [],
    auto_remove=True,
    dag=dag,
    priority_weight=priorities[0],
    retries=3,
    retry_delay=dt.timedelta(minutes=10)
)

generate_run_id >> init_config >> wps

select_wrf = DummyOperator(task_id='select-wrf', dag=dag)

for i in range(parallel_runs):
    suffix_run_id_wrf = PythonOperator(
        task_id='sfx-run-id-wrf%d' % i,
        python_callable=af_kube_utils.suffix_run_id,
        templates_dict={'run_id': '{{ ti.xcom_pull(task_ids=\'gen-run-id\') }}',
                        'run_id_suffix': str(i)},
        provide_context=True,
        dag=dag,
        priority_weight=priorities[i]
    )

    wrf_pod = get_base_pod()
    wrf_pod.metadata.name = 'wrf-pod-{{ ti.xcom_pull(task_ids=\'sfx-run-id-wrf%d\') }}' % i
    wrf_pod.spec.containers[0].name = 'wrf-cont-{{ ti.xcom_pull(task_ids=\'sfx-run-id-wrf%d\') }}' % i
    wrf_pod.spec.containers[0].image = wrf_image
    wrf_pod.spec.containers[0].command = ['/wrf/run_wrf.sh']
    wrf_pod.spec.containers[0].resources = client.V1ResourceRequirements(requests={'cpu': 4, 'memory': '6G'})
    wrf_pod.spec.containers[0].args = ['-i', '{{ ti.xcom_pull(task_ids=\'sfx-run-id-wrf%d\') }}' % i,
                                       '-c', '{{ ti.xcom_pull(task_ids=\'init-config\') }}',
                                       '-m', 'wrf',
                                       '-x', af_utils.get_base64_encoded_str(nl_wps),
                                       '-y', af_utils.get_base64_encoded_str(nl_inputs[i]),
                                       '-k', '/wrf/config/gcs.json',
                                       '-v', 'curwsl_nfs_1:/wrf/output',
                                       '-v', 'curwsl_archive_1:/wrf/archive',
                                       ]

    wrf = CurwGkeOperatorV2(
        task_id='wrf%d' % i,
        pod=wrf_pod,
        secret_list=secrets or [],
        auto_remove=True,
        dag=dag,
        poll_interval=dt.timedelta(minutes=5),
        priority_weight=priorities[i]
    )

    extract_pod = get_base_pod()
    extract_pod.metadata.name = 'wrf-ext-pod-{{ ti.xcom_pull(task_ids=\'sfx-run-id-wrf%d\') }}' % i
    extract_pod.spec.containers[0].name = 'wrf-ext-con-{{ ti.xcom_pull(task_ids=\'sfx-run-id-wrf%d\') }}' % i
    extract_pod.spec.containers[0].image = extract_image
    extract_pod.spec.containers[0].command = ['/wrf/extract_data_wrf.sh']
    extract_pod.spec.containers[0].resources = client.V1ResourceRequirements(requests={'cpu': 1})
    extract_pod.spec.containers[0].args = ['-i', '{{ ti.xcom_pull(task_ids=\'sfx-run-id-wrf%d\') }}' % i,
                                           '-c', '{{ ti.xcom_pull(task_ids=\'init-config\') }}',
                                           '-d', af_utils.get_base64_encoded_str('{}'),
                                           '-o', 'True',
                                           '-k', '/wrf/config/gcs.json',
                                           '-v', 'curwsl_nfs_1:/wrf/output',
                                           '-v', 'curwsl_archive_1:/wrf/archive',
                                           ]

    extract_wrf = CurwGkeOperatorV2(
        task_id='wrf%d-extract' % i,
        pod=extract_pod,
        secret_list=secrets,
        auto_remove=True,
        dag=dag,
        priority_weight=priorities[i]
    )

    extract_pod_no_push = get_base_pod()
    extract_pod_no_push.metadata.name = 'wrf-ext-pod-{{ ti.xcom_pull(task_ids=\'sfx-run-id-wrf%d\') }}' % i
    extract_pod_no_push.spec.containers[0].name = 'wrf-ext-con-{{ ti.xcom_pull(task_ids=\'sfx-run-id-wrf%d\') }}' % i
    extract_pod_no_push.spec.containers[0].image = extract_image
    extract_pod_no_push.spec.containers[0].command = ['/wrf/extract_data_wrf.sh']
    extract_pod_no_push.spec.containers[0].resources = client.V1ResourceRequirements(requests={'cpu': 1})
    extract_pod_no_push.spec.containers[0].args = ['-i', '{{ ti.xcom_pull(task_ids=\'sfx-run-id-wrf%d\') }}' % i,
                                                   '-c', '{{ ti.xcom_pull(task_ids=\'init-config\') }}',
                                                   '-d', af_utils.get_base64_encoded_str('{}'),
                                                   '-o', 'True',
                                                   '-k', '/wrf/config/gcs.json',
                                                   '-v', 'curwsl_nfs_1:/wrf/output',
                                                   '-v', 'curwsl_archive_1:/wrf/archive',
                                                   ]

    extract_wrf_no_data_push = CurwGkeOperatorV2(
        task_id='wrf%d-extract-no-data-push' % i,
        pod=extract_pod_no_push,
        secret_list=secrets,
        dag=dag,
        auto_remove=True,
        priority_weight=priorities[i]
    )

    check_data_push = BranchPythonOperator(
        task_id='check-data-push' + str(i),
        python_callable=af_kube_utils.check_data_push_callable,
        provide_context=True,
        op_args=[['wrf%d-extract-no-data-push' % i, 'wrf%d-extract-no-data-push' % i]],  # todo: change this
        dag=dag,
        priority_weight=priorities[i]
    )

    join_branch = DummyOperator(
        task_id='join-branch' + str(i),
        trigger_rule='one_success',
        dag=dag,
        priority_weight=priorities[i]
    )

    wps >> suffix_run_id_wrf >> wrf >> check_data_push
    check_data_push >> extract_wrf >> join_branch
    check_data_push >> extract_wrf_no_data_push >> join_branch
    join_branch >> select_wrf

select_wrf >> clean_up
