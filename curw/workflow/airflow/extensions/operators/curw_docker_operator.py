import json
import logging
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory
from docker import APIClient as Client, tls
import ast


class CurwDockerOperator(BaseOperator):
    """
    Execute a command inside a docker container.

    Additional functionality
        - container auto remove
        - container privileged

    A temporary directory is created on the host and mounted into a container to allow storing files
    that together exceed the default disk size of 10GB in a container. The path to the mounted
    directory can be accessed via the environment variable ``AIRFLOW_TMP_DIR``.

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
            api_version=None,
            command=None,
            cpus=1.0,
            docker_url='unix://var/run/docker.sock',
            environment=None,
            force_pull=False,
            mem_limit=None,
            network_mode=None,
            tls_ca_cert=None,
            tls_client_cert=None,
            tls_client_key=None,
            tls_hostname=None,
            tls_ssl_version=None,
            tmp_dir='/tmp/airflow',
            user=None,
            volumes=None,
            xcom_push=False,
            xcom_all=False,
            auto_remove=False,
            privileged=False,
            *args,
            **kwargs):

        super(CurwDockerOperator, self).__init__(*args, **kwargs)
        self.api_version = api_version
        self.command = command
        self.cpus = cpus
        self.docker_url = docker_url
        self.environment = environment or {}
        self.force_pull = force_pull
        self.image = image
        self.mem_limit = mem_limit
        self.network_mode = network_mode
        self.tls_ca_cert = tls_ca_cert
        self.tls_client_cert = tls_client_cert
        self.tls_client_key = tls_client_key
        self.tls_hostname = tls_hostname
        self.tls_ssl_version = tls_ssl_version
        self.tmp_dir = tmp_dir
        self.user = user
        self.volumes = volumes or []
        self.xcom_push = xcom_push
        self.xcom_all = xcom_all
        self.auto_remove = auto_remove
        self.priviledged = privileged

        self.cli = None
        self.container = None

    def execute(self, context):
        logging.info('Starting docker container from image ' + self.image)

        tls_config = None
        if self.tls_ca_cert and self.tls_client_cert and self.tls_client_key:
            tls_config = tls.TLSConfig(
                ca_cert=self.tls_ca_cert,
                client_cert=(self.tls_client_cert, self.tls_client_key),
                verify=True,
                ssl_version=self.tls_ssl_version,
                assert_hostname=self.tls_hostname
            )
            self.docker_url = self.docker_url.replace('tcp://', 'https://')

        self.cli = Client(base_url=self.docker_url, version=self.api_version, tls=tls_config)

        if ':' not in self.image:
            image = self.image + ':latest'
        else:
            image = self.image

        if self.force_pull or len(self.cli.images(name=image)) == 0:
            logging.info('Pulling docker image ' + image)
            for l in self.cli.pull(image, stream=True):
                output = json.loads(l.decode('utf-8'))
                logging.info("{}".format(output['status']))

        cpu_shares = int(round(self.cpus * 1024))

        with TemporaryDirectory(prefix='airflowtmp') as host_tmp_dir:
            self.environment['AIRFLOW_TMP_DIR'] = self.tmp_dir
            self.volumes.append('{0}:{1}'.format(host_tmp_dir, self.tmp_dir))

            cmd = self.get_command()
            logging.info('Creating container and running cmd:\n' + cmd)

            self.container = self.cli.create_container(
                command=cmd,
                cpu_shares=cpu_shares,
                environment=self.environment,
                host_config=self.cli.create_host_config(binds=self.volumes,
                                                        network_mode=self.network_mode,
                                                        auto_remove=self.auto_remove,
                                                        privileged=self.priviledged),
                image=image,
                mem_limit=self.mem_limit,
                user=self.user
            )
            self.cli.start(self.container['Id'])

            line = ''
            for line in self.cli.logs(container=self.container['Id'], stream=True):
                logging.info("{}".format(line.strip()))

            exit_code = self.cli.wait(self.container['Id'])
            if exit_code != 0:
                raise AirflowException('docker container failed')

            if self.xcom_push:
                return self.cli.logs(container=self.container['Id']) if self.xcom_all else str(line.strip())

    def get_command(self):
        if self.command is not None and self.command.strip().find('[') == 0:
            commands = ast.literal_eval(self.command)
        else:
            commands = self.command
        return commands

    def on_kill(self):
        if self.cli is not None:
            logging.info('Stopping docker container')
            self.cli.stop(self.container['Id'])
