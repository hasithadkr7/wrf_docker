import random
import time
import datetime as dt
import logging

from kubernetes import client, config
from kubernetes.client import rest

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from curw.workflow.airflow import utils as af_utils


class CurwGkeOperatorV2Exception(Exception):
    pass


K8S_API_VERSION_TAG = 'v1'


class CurwGkeOperatorV2(BaseOperator):
    """

    """
    template_fields = ['kube_config_path', 'pod_name', 'namespace', 'container_names', 'container_commands',
                       'container_args_lists']

    @apply_defaults
    def __init__(
            self,
            pod,
            pod_name=None,
            namespace=None,
            kube_config_path=None,
            secret_list=None,
            api_version=None,
            auto_remove=False,
            poll_interval=dt.timedelta(seconds=60),
            cleanup_on_exec=False,
            *args,
            **kwargs):

        super(CurwGkeOperatorV2, self).__init__(*args, **kwargs)
        self.api_version = api_version or K8S_API_VERSION_TAG
        self.kube_config_path = kube_config_path
        self.pod = pod

        self.pod_name = pod_name or self.pod.metadata.name
        self.namespace = namespace or self.pod.metadata.namespace or 'default'

        self.container_names = []
        self.container_commands = []
        self.container_args_lists = []
        for c in pod.spec.containers:
            self.container_names.append(c.name)
            self.container_commands.append(c.command)
            self.container_args_lists.append(c.args)

        self.auto_remove = auto_remove
        self.secrets_list = secret_list or []

        self.kube_client = None

        self.poll_interval = poll_interval
        self.cleanup_on_exec = cleanup_on_exec

    def _wait_for_pod_completion(self):
        start_t = dt.datetime.now()
        pod_started = False
        deletable = True
        time.sleep(self.poll_interval.seconds)
        while True:
            try:
                pod = self.kube_client.read_namespaced_pod_status(name=self.pod.metadata.name,
                                                                  namespace=self.pod.metadata.namespace)
                pod_started = True
                logging.info(
                    "Pod status: %s elapsed time: %s" % (pod.status.phase, str(dt.datetime.now() - start_t)))
                status = pod.status.phase
                if status == 'Succeeded' or status == 'Failed':
                    log = 'Pod log:\n' + self.kube_client.read_namespaced_pod_log(name=self.pod.metadata.name,
                                                                                  namespace=self.pod.metadata.namespace,
                                                                                  timestamps=True, pretty='true')
                    logging.info(
                        'Pod exited! %s %s %s\n%s' % (self.pod.metadata.namespace, self.pod.metadata.name, status, log))
                    return deletable
            except rest.ApiException as e:
                if e.reason == 'Unauthorized':
                    random_wait_secs = random.randint(0, self.poll_interval.seconds)
                    logging.warning('API token expired! waiting for %d sec' % random_wait_secs)
                    time.sleep(random_wait_secs)

                    logging.info('Initializing kubernetes config from file ' + str(self.kube_config_path))
                    config.load_kube_config(config_file=self.kube_config_path)

                    logging.info('Initializing kubernetes client for API version ' + self.api_version)
                    if self.api_version.lower() == K8S_API_VERSION_TAG:
                        self.kube_client = client.CoreV1Api()
                    else:
                        raise CurwGkeOperatorV2Exception('Unsupported API version ' + self.api_version)
                    # iterate again with the refreshed token
                    continue
                else:
                    raise CurwGkeOperatorV2Exception('Error in polling pod %s:%s' % (self.pod.metadata.name, str(e)))
            except Exception as e:
                if pod_started:
                    raise CurwGkeOperatorV2Exception('Error in polling pod %s:%s' % (self.pod.metadata.name, str(e)))
                else:
                    logging.warning('Pod has not started yet %s:%s' % (self.pod.metadata.name, str(e)))

            time.sleep(self.poll_interval.seconds)

    def _create_secrets(self):
        if self.kube_client is not None:
            avail_secrets = [i.metadata.name for i in
                             self.kube_client.list_namespaced_secret(namespace=self.pod.metadata.namespace).items]
            for secret in self.secrets_list:
                if secret.metadata.name not in avail_secrets:
                    self.kube_client.create_namespaced_secret(namespace=self.pod.metadata.namespace, body=secret)
                else:
                    logging.info('Secret exists ' + secret.metadata.name)

    def _create_pod(self):
        try:
            self.kube_client.create_namespaced_pod(namespace=self.pod.metadata.namespace, body=self.pod)
        except rest.ApiException as e:
            if e.reason == 'Conflict':
                logging.warning('Pod already exists ' + self.pod.metadata.name)
                if self.cleanup_on_exec:
                    logging.info('Cleaning up pod ' + self.pod.metadata.name)
                    self.on_kill()
                    self._create_pod()
            else:
                raise CurwGkeOperatorV2Exception('Error in creating pod %s:%s' % (self.pod.metadata.name, str(e)))

    def execute(self, context):
        logging.info('Updating pod with templated fields')
        self.pod.metadata.name = af_utils.sanitize_name(self.pod_name)
        self.pod.metadata.namespace = af_utils.sanitize_name(self.namespace)
        for i in range(len(self.container_names)):
            self.pod.spec.containers[i].name = af_utils.sanitize_name(self.container_names[i])
            self.pod.spec.containers[i].command = self.container_commands[i]
            self.pod.spec.containers[i].args = self.container_args_lists[i]

        self._initialize_kube_config()

        logging.info('Creating secrets')
        self._create_secrets()

        logging.info('Creating namespaced pod ' + self.pod.metadata.name)
        logging.debug('Pod config ' + str(self.pod))
        self._create_pod()

        logging.info('Waiting for pod ' + self.pod.metadata.name)
        deletable = self._wait_for_pod_completion()

        if self.auto_remove and deletable:
            self.on_kill()

    def _initialize_kube_config(self):
        logging.info('Initializing kubernetes config from file ' + str(self.kube_config_path))
        config.load_kube_config(config_file=self.kube_config_path)

        logging.info('Initializing kubernetes client for API version ' + self.api_version)
        if self.api_version.lower() == K8S_API_VERSION_TAG:
            self.kube_client = client.CoreV1Api()
        else:
            raise CurwGkeOperatorV2Exception('Unsupported API version ' + self.api_version)

    def on_kill(self):
        if self.kube_client is not None:
            logging.info('Stopping kubernetes pod')
            i = 0
            while i < 5:
                try:
                    self.kube_client.delete_namespaced_pod(name=self.pod.metadata.name,
                                                           namespace=self.pod.metadata.namespace,
                                                           body=client.V1DeleteOptions())
                    return
                except rest.ApiException as e:
                    if e.reason == 'Unauthorized':
                        logging.warning('API token expired!')
                        self._initialize_kube_config()
                    if i > 0:
                        time.sleep(random.randint(0, 10))
                    i += 1
                    continue
