from airflow.utils.decorators import apply_defaults

from airflow.models import BaseOperator


class CurwSwarmOperator(BaseOperator):
    """

    http://docker-py.readthedocs.io/en/stable/services.html

    :param image (str) – The image name to use for the containers.
    :param command (list of str or str) – Command to run.
    :param args (list of str) – Arguments to the command.
    :param constraints (list of str) – Placement constraints.
    :param container_labels (dict) – Labels to apply to the container.
    :param endpoint_spec (EndpointSpec) – Properties that can be configured to access and load balance a service. Default: None.
    :param env (list of str) – Environment variables, in the form KEY=val.
    :param hostname (string) – Hostname to set on the container.
    :param labels (dict) – Labels to apply to the service.
    :param log_driver (str) – Log driver to use for containers.
    :param log_driver_options (dict) – Log driver options.
    :param mode (ServiceMode) – Scheduling mode for the service. Default:None
    :param mounts (list of str) – Mounts for the containers, in the form source:target:options, where options is either ro or rw.
    :param name (str) – Name to give to the service.
    :param networks (list of str) – List of network names or IDs to attach the service to. Default: None.
    :param resources (Resources) – Resource limits and reservations.
    :param restart_policy (RestartPolicy) – Restart policy for containers.
    :param secrets (list of docker.types.SecretReference) – List of secrets accessible to containers for this service.
    :param stop_grace_period (int) – Amount of time to wait for containers to terminate before forcefully killing them.
    :param update_config (UpdateConfig) – Specification for the update strategy of the service. Default: None
    :param user (str) – User to run commands as.
    :param workdir (str) – Working directory for commands to run.
    :param tty (boolean) – Whether a pseudo-TTY should be allocated.
    :param groups (list) – A list of additional groups that the container process will run as.
    :param open_stdin (boolean) – Open stdin
    :param read_only (boolean) – Mount the container’s root filesystem as read only.
    :param stop_signal (string) – Set signal to stop the service’s containers
    :param healthcheck (Healthcheck) – Healthcheck configuration for this service.
    :param hosts (dict) – A set of host to IP mappings to add to the container’s hosts file.
    :param dns_config (DNSConfig) – Specification for DNS related configurations in resolver configuration file.
    :param configs (list) – List of ConfigReference that will be exposed to the service.
    :param privileges (Privileges) – Security options for the service’s containers.
    """

    template_fields = ('command',)
    template_ext = ('.sh', '.bash',)

    @apply_defaults
    def __init__(
            self,
            image,
            command,
            args_str,
            constraints,
            container_labels,
            endpoint_spec,
            env,
            hostname,
            labels,
            log_driver,
            log_driver_options,
            mode,
            mounts,
            name,
            networks,
            resources,
            restart_policy,
            secrets,
            stop_grace_period,
            update_config,
            user,
            workdir,
            tty,
            groups,
            open_stdin,
            read_only,
            stop_signal,
            healthcheck,
            hosts,
            dns_config,
            configs,
            privileges,
            *args,
            **kwargs):

        super(CurwSwarmOperator, self).__init__(*args, **kwargs)
        self.image = image
        self.command = command
        self.args = args
        self.constraints = constraints
        self.container_labels = container_labels
        self.endpoint_spec = endpoint_spec
        self.env = env
        self.hostname = hostname
        self.labels = labels
        self.log_driver = log_driver
        self.log_driver_options = log_driver_options
        self.mode = mode
        self.mounts = mounts
        self.name = name
        self.networks = networks
        self.resources = resources
        self.restart_policy = restart_policy
        self.secrets = secrets
        self.stop_grace_period = stop_grace_period
        self.update_config = update_config
        self.user = user
        self.workdir = workdir
        self.tty = tty
        self.groups = groups
        self.open_stdin = open_stdin
        self.read_only = read_only
        self.stop_signal = stop_signal
        self.healthcheck = healthcheck
        self.hosts = hosts
        self.dns_config = dns_config
        self.configs = configs
        self.privileges = privileges

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
