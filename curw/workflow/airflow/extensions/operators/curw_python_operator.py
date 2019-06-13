import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from curw.workflow.airflow.extensions.tasks import CurwTask


class CurwPythonOperator(BaseOperator):
    template_fields = ('pre_args', 'process_args', 'post_args', 'pre_kwargs', 'process_kwargs', 'post_kwargs')
    template_ext = tuple()
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
            self,
            curw_task,
            init_args=None,
            init_kwargs=None,
            pre_args=None,
            pre_kwargs=None,
            process_args=None,
            process_kwargs=None,
            post_args=None,
            post_kwargs=None,
            provide_context=False,
            test_mode=False,
            *args, **kwargs):
        super(CurwPythonOperator, self).__init__(*args, **kwargs)
        self.curw_task = curw_task
        self.curw_task_init_args = init_args or []
        self.curw_task_init_kwargs = init_kwargs or {}
        self.pre_args = pre_args or []
        self.pre_kwargs = pre_kwargs or {}
        self.process_args = process_args or []
        self.process_kwargs = process_kwargs or {}
        self.post_args = post_args or []
        self.post_kwargs = post_kwargs or {}
        self.provide_context = provide_context
        self.test_mode = test_mode

    def execute(self, context):
        if not issubclass(self.curw_task, CurwTask):
            raise CurwAriflowOperatorsException('Provided task is not a CurwTask')

        if self.provide_context:
            self.pre_kwargs.update(context)
            self.post_kwargs.update(context)
            self.process_kwargs.update(context)

        curw_task_instance = self.curw_task(*self.curw_task_init_args, **self.curw_task_init_kwargs)

        if not self.test_mode:
            return_value = curw_task_instance.pre_process(*self.pre_args, **self.pre_kwargs)
            logging.info("Preporcess Done! Returned value was: " + str(return_value))

            return_value = curw_task_instance.process(*self.process_args, **self.process_kwargs)
            logging.info("Process Done! Returned value was: " + str(return_value))

            return_value = curw_task_instance.post_process(*self.post_args, **self.post_kwargs)
            logging.info("Post process Done! Returned value was: " + str(return_value))
        else:
            logging.info('Running on test mode')

        return 'DONE'


class CurwAriflowOperatorsException(Exception):
    def __init__(self, msg):
        self.msg = msg
        Exception.__init__(self, msg)
