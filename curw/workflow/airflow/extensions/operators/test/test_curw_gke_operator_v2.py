import base64
import logging
from datetime import timedelta

import os
from kubernetes import client

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from curw.workflow.airflow.extensions.operators.curw_gke_operator_v2 import CurwGkeOperatorV2

# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
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


def get_resource_name(resource, context):
    return '--'.join([resource.lower(), str(context['ti'].dag_id).lower(), str(context['ti'].task_id).lower(),
                      str(context['ti'].job_id).lower()])


def read_file(path):
    with open(path, 'r') as f:
        return f.read()


def get_base64_encoded_str(s):
    return base64.b64encode(s.encode()).decode()


run_id = "wrf-kube-test_2018-02-19_18:00_0000"

wrf_config = '''{
"archive_dir": "/wrf/archive",
"geog_dir": "/wrf/geog/wrf_391_geog",
"gfs_dir": "/wrf/gfs",
"nfs_dir": "/wrf/output",
"period": 0.25,
"procs": 4,
"run_id": "wrf-kube-test_2018-02-19_18:00_0000",
"start_date": "2018-02-19_18:00",
"wrf_home": "/wrf"
}'''

namelist_wps = '''&share
 wrf_core = 'ARW',
 max_dom = 3,
 start_date = 'YYYY1-MM1-DD1_hh1:mm1:00','YYYY1-MM1-DD1_hh1:mm1:00','YYYY1-MM1-DD1_hh1:mm1:00',
 end_date   = 'YYYY2-MM2-DD2_hh2:mm2:00','YYYY2-MM2-DD2_hh2:mm2:00','YYYY2-MM2-DD2_hh2:mm2:00',
 interval_seconds = 10800,
 io_form_geogrid = 2,
/

&geogrid
 parent_id         =   1,   1, 2,
 parent_grid_ratio =   1,   3, 3,
 i_parent_start    =   1,  24, 35,
 j_parent_start    =   1,  26, 35,
 e_we              =  80, 103, 100,
 e_sn              =  90, 121, 163,
 geog_data_res     = '10m','5m','2m',
 dx = 27000,
 dy = 27000,
 map_proj = 'mercator',
 ref_lat   =  7.697,
 ref_lon   =  80.774,
 truelat1  =  7.697,
 truelat2  =  0,
 stand_lon =  80.774,
 geog_data_path = 'GEOG'
/

&ungrib
 out_format = 'WPS',
 prefix = 'FILE',
/

&metgrid
 fg_name = 'FILE'
 io_form_metgrid = 2, 
/

'''

namelist_input = ''' &time_control
 run_days                            = RD0,
 run_hours                           = RH0,
 run_minutes                         = RM0,
 run_seconds                         = 0,
 start_year                          = YYYY1, YYYY1,  YYYY1,
 start_month                         = MM1, MM1,  MM1,
 start_day                           = DD1, DD1,  DD1,
 start_hour                          = hh1,   hh1,   hh1,
 start_minute                        = mm1,   mm1,   mm1,
 start_second                        = 00,   00,   00,
 end_year                            = YYYY2, YYYY2,  YYYY2,
 end_month                           = MM2, MM2,  MM2,
 end_day                             = DD2, DD2,  DD2,
 end_hour                            = hh2,   hh2,   hh2,
 end_minute                          = mm2,   mm2,   mm2,
 end_second                          = 00,   00,   00,
 interval_seconds                    = 10800
 input_from_file                     = .true.,.true.,.true.,
 history_interval                    = hi1,  hi2,   hi3,
 frames_per_outfile                  = 1000, 1000, 1000,
 restart                             = .false.,
 restart_interval                    = 5000,
 io_form_history                     = 2
 io_form_restart                     = 2
 io_form_input                       = 2
 io_form_boundary                    = 2
 debug_level                         = 0
 /

 &domains
 time_step                           = 180,
 time_step_fract_num                 = 0,
 time_step_fract_den                 = 1,
 max_dom                             = 3,
 e_we                                = 80,    103,   100,
 e_sn                                = 90,    121,    163,
 e_vert                              = 30,    30,    30,
 p_top_requested                     = 5000,
 num_metgrid_levels                  = 32,
 num_metgrid_soil_levels             = 4,
 dx                                  = 27000, 9000,  3000,
 dy                                  = 27000, 9000,  3000,
 grid_id                             = 1,     2,     3,
 parent_id                           = 1,     1,     2,
 i_parent_start                      = 1,     24,    35,
 j_parent_start                      = 1,     26,    35,
 parent_grid_ratio                   = 1,     3,     3,
 parent_time_step_ratio              = 1,     3,     3,
 feedback                            = 1,
 smooth_option                       = 0
 /

 &physics
 mp_physics                          = 3,     3,     3,
 ra_lw_physics                       = 1,     1,     1,
 ra_sw_physics                       = 1,     1,     1,
 radt                                = 30,    10,    10,
 sf_sfclay_physics                   = 1,     1,     1,
 sf_surface_physics                  = 2,     2,     2,
 bl_pbl_physics                      = 1,     1,     1,
 bldt                                = 0,     0,     0,
 cu_physics                          = 1,     1,     1,
 cudt                                = 5,     5,     5,
 isfflx                              = 1,
 ifsnow                              = 0,
 icloud                              = 1,
 surface_input_source                = 1,
 num_soil_layers                     = 4,
 sf_urban_physics                    = 0,     0,     0,
 /

 &fdda
 /

 &dynamics
 w_damping                           = 0,
 diff_opt                            = 1,
 km_opt                              = 4,
 diff_6th_opt                        = 0,      0,      0,
 diff_6th_factor                     = 0.12,   0.12,   0.12,
 base_temp                           = 290.
 damp_opt                            = 0,
 zdamp                               = 5000.,  5000.,  5000.,
 dampcoef                            = 0.2,    0.2,    0.2
 khdif                               = 0,      0,      0,
 kvdif                               = 0,      0,      0,
 non_hydrostatic                     = .true., .true., .true.,
 moist_adv_opt                       = 1,      1,      1,     
 scalar_adv_opt                      = 1,      1,      1,     
 /

 &bdy_control
 spec_bdy_width                      = 5,
 spec_zone                           = 1,
 relax_zone                          = 4,
 specified                           = .true., .false.,.false.,
 nested                              = .false., .true., .true.,
 /

 &grib2
 /

 &namelist_quilt
 nio_tasks_per_group = 0,
 nio_groups = 1,
 /

'''

image = 'us.gcr.io/uwcc-160712/curw-wrf-391'
kube_config_path = None
api_version = None
# command = ['sh']
# command_args = ['-c', 'ls /wrf; ls /wrf/config; cat /wrf/config/gcs.json']
command = ['/wrf/run_wrf.sh']
command_args = ['-i', '%s' % run_id,
                '-c', '%s' % get_base64_encoded_str(wrf_config),
                '-m', 'all',
                '-x', '%s' % get_base64_encoded_str(namelist_wps),
                '-y', '%s' % get_base64_encoded_str(namelist_input),
                '-k', '/wrf/config/gcs.json',
                '-v', 'curwsl_nfs_1:/wrf/output',
                '-v', 'curwsl_archive_1:/wrf/archive',
                ]

logging.info('Creating container spec for image ' + image)
container = client.V1Container(name='test-container',
                               image_pull_policy='IfNotPresent')
container.image = image
container.command = command
container.args = command_args
container.volume_mounts = [client.V1VolumeMount(mount_path='/wrf/geog', name='test-vol'),
                           client.V1VolumeMount(mount_path='/wrf/config', name='test-sec-vol')]
container.resources = client.V1ResourceRequirements(requests={'cpu': 4})
container.security_context = client.V1SecurityContext(privileged=True)

logging.info('Initializing pod')
pod = client.V1Pod()

logging.info('Creating pod metadata ')
pod_metadata = client.V1ObjectMeta(name='test-pod')
pod.metadata = pod_metadata

logging.info('Creating volume list')
vols = [client.V1Volume(name='test-vol',
                        gce_persistent_disk=client.V1GCEPersistentDiskVolumeSource(pd_name='wrf-geog-disk1',
                                                                                   read_only=True)),
        client.V1Volume(name='test-sec-vol',
                        secret=client.V1SecretVolumeSource(secret_name='google-app-creds'))]

logging.info('Creating pod spec')
pod_spec = client.V1PodSpec(containers=[container],
                            restart_policy='OnFailure',
                            volumes=vols, )
pod.spec = pod_spec

google_app_creds = client.V1Secret(kind='Secret', type='Opaque', metadata=client.V1ObjectMeta(name='google-app-creds'),
                                   data={
                                       'gcs.json': get_base64_encoded_str(
                                           read_file(os.getenv('GOOGLE_APPLICATION_CREDENTIALS')))
                                   })
secrets = [google_app_creds]

dag = DAG(
    'gke-test2',
    default_args=default_args,
    schedule_interval=timedelta(days=1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = CurwGkeOperatorV2(
    pod=pod,
    secret_list=secrets or [],
    pod_name='test-pod-1',
    auto_remove=False,
    task_id='wrf',
    dag=dag,
)

t1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

dag.doc_md = __doc__

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    dag=dag)

templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t1)
