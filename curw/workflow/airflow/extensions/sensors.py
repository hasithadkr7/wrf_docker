import logging
import os

from airflow.models import Variable
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from curw.rainfall.wrf.execution.executor import WrfConfig
from curw.rainfall.wrf import utils


class CurwWrfFileLockSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, config_key, *args, **kwargs):
        self.config_key = config_key
        self.config = WrfConfig()
        super(CurwWrfFileLockSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        file_path = os.path.join(utils.get_em_real_dir(self.get_config().get('wrf_home')), 'wrf.lock')
        logging.info('Poking %s' % file_path)
        if os.path.exists(file_path) and os.path.isfile(file_path):
            return False
        else:
            return True

    def get_config(self):
        if self.config.is_empty():
            self.config = WrfConfig(Variable.get(self.config_key, deserialize_json=True))
        return self.config

