import logging
import os

from kubernetes import client, config, watch

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# constants
from curw.rainfall.wrf import utils

V1_API_VERSION_TAG = 'V1'


def get_resource_name(resource, context):
    return '--'.join([resource.lower(), str(context['ti'].dag_id).lower(), str(context['ti'].task_id).lower(),
                      str(context['ti'].job_id).lower()])


def validate_container_image(image_name):
    if image_name.find('gcr.io') >= 0:
        return image_name
    else:
        raise CurwGkeOperatorException('Invalid container image ' + image_name)


class CurwGkeOperatorException(Exception):
    pass


class CurwGkeOperator(BaseOperator):
    """
    :param image: Docker image from which to create the container.
    :type image: str
    :param api_version: Remote API version.
    :type api_version: str
    :param command: Command to be run in the container.
    :type command: str or list
    :param cpus: Number of CPUs to assign to the container.
        This value gets multiplied with 1024. See
        https://docs.docker.com/engine/reference/run/#cpu-share-constraint
    :type cpus: float
    :param docker_url: URL of the host running the docker daemon.
    :type docker_url: str
    :param environment: Environment variables to set in the container.
    :type environment: dict
    :param force_pull: Pull the docker image on every run.
    :type force_pull: bool
    :param mem_limit: Maximum amount of memory the container can use. Either a float value, which
        represents the limit in bytes, or a string like ``128m`` or ``1g``.
    :type mem_limit: float or str
    :param network_mode: Network mode for the container.
    :type network_mode: str
    :param tls_ca_cert: Path to a PEM-encoded certificate authority to secure the docker connection.
    :type tls_ca_cert: str
    :param tls_client_cert: Path to the PEM-encoded certificate used to authenticate docker client.
    :type tls_client_cert: str
    :param tls_client_key: Path to the PEM-encoded key used to authenticate docker client.
    :type tls_client_key: str
    :param tls_hostname: Hostname to match against the docker server certificate or False to
        disable the check.
    :type tls_hostname: str or bool
    :param tls_ssl_version: Version of SSL to use when communicating with docker daemon.
    :type tls_ssl_version: str
    :param tmp_dir: Mount point inside the container to a temporary directory created on the host by
        the operator. The path is also made available via the environment variable
        ``AIRFLOW_TMP_DIR`` inside the container.
    :type tmp_dir: str
    :param user: Default user inside the docker container.
    :type user: int or str
    :param volumes: List of volumes to mount into the container, e.g.
        ``['/host/path:/container/path', '/host/path2:/container/path2:ro']``.
    :param xcom_push: Does the stdout will be pushed to the next step using XCom.
           The default is False.
    :type xcom_push: bool
    :param xcom_all: Push all the stdout or just the last line. The default is False (last line).
    :type xcom_all: bool
    """
    template_fields = ('command',)
    template_ext = ('.sh', '.bash',)

    @apply_defaults
    def __init__(
            self,
            image,
            kube_config_path=None,
            api_version=None,
            command=None,
            command_args=None,
            cpus=1.0,
            environment=None,
            force_pull=False,
            mem_limit=None,
            network_mode=None,
            tmp_dir='/tmp/airflow',
            user=None,
            volumes=None,
            auto_remove=False,
            priviliedged=False,
            namespace='default',
            export_google_app_creds=None,
            *args,
            **kwargs):

        super(CurwGkeOperator, self).__init__(*args, **kwargs)
        self.api_version = api_version or V1_API_VERSION_TAG
        self.command = command or []
        self.command_args = command_args or []
        self.cpus = cpus
        self.kube_config_path = kube_config_path
        self.environment = environment or {}
        self.image_pull_policy = 'Always' if force_pull else 'IfNotPresent'
        self.image = validate_container_image(image)
        self.mem_limit = mem_limit
        self.network_mode = network_mode
        self.tmp_dir = tmp_dir
        self.user = user
        self.volumes = volumes or []
        self.auto_remove = auto_remove
        self.priviledged = priviliedged
        self.namespace = namespace
        self.export_google_app_creds = export_google_app_creds

        self.kube_client = None
        self.pod = None

    def _wait_for_pod_completion(self):
        w = watch.Watch()
        for event in w.stream(self.kube_client.list_namespaced_pod, self.namespace):
            logging.info(event)
            phase = event['object'].status.phase
            if phase == 'Succeeded':
                logging.info('Pod completed successfully!')
                w.stop()
                break
            elif phase == 'Failed':
                logging.error('Pod failed!')
                w.stop()
                break

    @staticmethod
    def _get_google_app_credentials():
        g_app_cred_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        if utils.file_exists_nonempty(g_app_cred_path):
            with open(g_app_cred_path, 'r') as f:
                return f.read()
        else:
            return None

    def execute(self, context):
        logging.info('Initializing kubernetes config from file ' + str(self.kube_config_path))
        config.load_kube_config(config_file=self.kube_config_path)

        logging.info('Initializing kubernetes client for API version ' + self.api_version)
        if self.api_version.upper() == V1_API_VERSION_TAG:
            self.kube_client = client.CoreV1Api()
        else:
            raise CurwGkeOperatorException('Unsupported API version ' + self.api_version)

        logging.info('Creating container spec for image ' + self.image)
        container_name = get_resource_name('container', context)
        logging.debug('Container name: ' + container_name)
        container = client.V1Container(name=container_name,
                                       image_pull_policy=self.image_pull_policy)
        container.image = self.image
        container.command = self.command
        container.args = self.command_args

        logging.info('Initializing pod')
        self.pod = client.V1Pod()

        logging.info('Creating pod metadata ')
        pod_name = get_resource_name('pod', context)
        logging.debug('Pod name: ' + pod_name)
        pod_metadata = client.V1ObjectMeta(name=pod_name)
        self.pod.metadata = pod_metadata

        logging.info('Creating pod spec')
        pod_spec = client.V1PodSpec(containers=[container],
                                    restart_policy='OnFailure')
        self.pod.spec = pod_spec

        logging.info('Creating namespaced pod')
        self.kube_client.create_namespaced_pod(namespace=self.namespace, body=self.pod)

        logging.info('Waiting for pod completion')
        self._wait_for_pod_completion()

        logging.info('Pod log:\n' + self.kube_client.read_namespaced_pod_log(name=pod_name, namespace=self.namespace,
                                                                             timestamps=True, pretty='true'))
        if self.auto_remove:
            self.on_kill()

    def on_kill(self):
        if self.kube_client is not None:
            logging.info('Stopping kubernetes pod')
            self.kube_client.delete_namespaced_pod(name=self.pod.metadata.name, namespace=self.namespace,
                                                   body=client.V1DeleteOptions())
