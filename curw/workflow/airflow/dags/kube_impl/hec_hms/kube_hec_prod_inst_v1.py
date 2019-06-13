import datetime as dt
import json
import logging
import os

from kubernetes import client

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from curw.workflow.airflow import utils as af_utils
from curw.workflow.airflow.dags.kube_impl import utils as af_kube_utils
from curw.workflow.airflow.extensions.operators.curw_gke_operator_v2 import CurwGkeOperatorV2

dag_name = 'kube_hec_prod_inst_v1'
queue = 'docker_prod_queue'  # reusing docker queue
schedule_interval = None

# parallel_runs = 3
parallel_runs = 1
priorities = [1, 1, 1]

run_id_prefix = 'kube-hec-prod-inst'

# aiflow variable keys
hec_config_var = 'kube_hec_config_inst'

# kube images
hec_hms_image = 'us.gcr.io/uwcc-160712/curw-hec-hms-420'

# volumes and mounts
vols = [client.V1Volume(name='sec-vol', secret=client.V1SecretVolumeSource(secret_name='google-app-creds'))]

vol_mounts = [client.V1VolumeMount(mount_path='/hec-hms/config', name='sec-vol')]

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
            restart_policy='Never',
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
    description='Running multiple HEC-HMSs using Google Kubernetes Engine',
    schedule_interval=schedule_interval)

hec_config = Variable.get(hec_config_var, deserialize_json=True)

for i in range(parallel_runs):
    generate_run_id = PythonOperator(
        task_id='gen-run-id',
        python_callable=af_kube_utils.generate_random_run_id,
        op_args=[run_id_prefix],
        op_kwargs={"suffix": "%04d" % i},
        provide_context=True,
        dag=dag
    )

    logging.info('Initializing hec-hms pod')
    hec_pod = get_base_pod()
    hec_pod.metadata.name = 'kube-pod-{{ ti.xcom_pull(task_ids=\'gen-run-id\') }}'
    hec_pod.spec.containers[0].name = 'kube-cont-{{ ti.xcom_pull(task_ids=\'gen-run-id\') }}'
    hec_pod.spec.containers[0].image = hec_hms_image
    hec_pod.spec.containers[0].command = ['/run.sh']
    hec_pod.spec.containers[0].resources = client.V1ResourceRequirements(requests={'cpu': 2, 'memory': '4G'})
    hec_pod.spec.containers[0].args = ['-i', '{{ ti.xcom_pull(task_ids=\'gen-run-id\') }}',
                                       '-w', hec_config['WRF_ID'],
                                       '-f', '%s' % af_utils.get_base64_encoded_str('-e'),
                                       '-c', '/hec-hms/CONFIG.json',
                                       '-y', '%s' % af_utils.get_base64_encoded_str(json.dumps(hec_config_var)),
                                       '-k', '/hec-hms/config/gcs.json',
                                       '-v', 'curwsl_nfs_1:/wrf/output',
                                       '-v', 'curwsl_archive_1:/wrf/archive',
                                       ]

    hec = CurwGkeOperatorV2(
        task_id='hec',
        pod=hec_pod,
        secret_list=secrets or [],
        auto_remove=False,
        dag=dag,
        priority_weight=priorities[0],
        retries=3,
        retry_delay=dt.timedelta(minutes=10),
    )

    generate_run_id >> hec
