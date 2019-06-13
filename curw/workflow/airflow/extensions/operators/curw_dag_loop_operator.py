import datetime as dt

from sqlalchemy import select, and_, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import MetaData

from airflow import settings
from airflow.models import BaseOperator, DagBag, SkipMixin
from airflow.operators.dagrun_operator import DagRunOrder
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State

Base = declarative_base()


class CurwDagLoopOperator(BaseOperator, SkipMixin):
    """
    Triggers a DAG run for a specified ``dag_id`` if a criteria is met

    :param trigger_dag_id: the dag_id to trigger
    :type trigger_dag_id: str
    :param python_callable: a reference to a python function that will be
        called while passing it the ``context`` object and a placeholder
        object ``obj`` for your callable to fill and return if you want
        a DagRun created. This ``obj`` object contains a ``run_id`` and
        ``payload`` attribute that you can modify in your function.
        The ``run_id`` should be a unique identifier for that DAG run, and
        the payload has to be a picklable object that will be made available
        to your tasks while executing that DAG run. Your function header
        should look like ``def foo(context, dag_run_obj):``
    :type python_callable: python callable
    """
    template_fields = ['loop_id', ]
    template_ext = tuple()
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
            self,
            python_callable,
            skip_downstream=True,
            default_loop_retries=1,
            *args, **kwargs):
        super(CurwDagLoopOperator, self).__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.skip_downstream = skip_downstream
        self.default_loop_retries = default_loop_retries
        self.loop_count = 0
        self.loop_id = None

    def execute(self, context, **kwargs):
        """
        if loop count > 0
        :param context:
        :param kwargs:
        :return:
        """
        self.loop_id = 'loop_' + context['execution_date'].strftime('%Y_%m_%d_%H_%M_%S')
        self.log.info('Loop id ' + self.loop_id)
        loop_count = self.get_loop_count()

        if loop_count != 0:
            loop_dag_run_id = '__'.join(['loop', self.dag_id, self.loop_id, str(loop_count)])
            self.log.info('DagRun Loop id ' + loop_dag_run_id)
            dro = DagRunOrder(run_id=loop_dag_run_id)
            dro = self.python_callable(context, dro)
            if dro:
                self.log.info('Loop criteria met. Loop count %d' % loop_count)
                self.dag.create_dagrun(
                    run_id=dro.run_id,
                    state=State.RUNNING,
                    conf=dro.payload,
                    execution_date=context['execution_date'] + dt.timedelta(microseconds=1) if context[
                        'execution_date'] else None,
                    external_trigger=True)

                self.log.info('Decrementing loop from %d' % loop_count)
                self.set_loop_count(loop_count - 1)

                if self.skip_downstream:
                    self.log.info('Skipping the downstream tasks')
                    downstream_tasks = context['task'].get_flat_relatives(upstream=False)
                    self.log.debug("Downstream task_ids %s", downstream_tasks)

                    if downstream_tasks:
                        self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)
            else:
                self.log.info("Loop criteria not met. Continuing the downstream tasks")
                self.delete_loop_count()
        else:
            self.log.info('Loop count is 0. Continuing the downstream tasks ')
            self.delete_loop_count()

    def delete_loop_count(self):
        session = settings.Session()
        meta = MetaData(session.connection(), reflect=True)
        loop_table = meta.tables['loop']
        try:
            del_st = loop_table.delete().where(
                and_(loop_table.c.id == self.loop_id, loop_table.c.dag == self.dag_id))
            session.execute(del_st)
            session.commit()
        finally:
            session.close()

    def get_loop_count(self):
        session = settings.Session()
        meta = MetaData(session.connection(), reflect=True)

        loop_table = meta.tables['loop']
        select_st = select([loop_table.c.count]).where(
            and_(loop_table.c.id == self.loop_id, loop_table.c.dag == self.dag_id))
        result = session.execute(select_st).fetchall()

        try:
            if len(result) == 0:
                insert_st = loop_table.insert().values(id=self.loop_id, dag=self.dag_id,
                                                       count=self.default_loop_retries)
                session.execute(insert_st)
                session.commit()
                return self.default_loop_retries
            elif len(result) == 1:
                return dict(result[0])['count']
            else:
                raise CurwDagLoopOperatorException(
                    'Multiple entries %d in the loop table for %s %s' % (len(result), self.dag_id, self.loop_id))
        finally:
            session.close()

    def set_loop_count(self, count):
        session = settings.Session()
        meta = MetaData(session.connection(), reflect=True)
        loop_table = meta.tables['loop']

        try:
            update_st = update(loop_table).where(
                and_(loop_table.c.id == self.loop_id, loop_table.c.dag == self.dag_id)).values(count=count)
            session.execute(update_st)
            session.commit()
        finally:
            session.close()

        return count

    def on_kill(self):
        self.log.info('Cleaning up loop metadata on kill')
        self.delete_loop_count()


class CurwDagLoopOperatorException(Exception):
    pass

#
# class LoopEntry(Base):
#     __tablename__ = "loop"
#
#     id = Column(String(250), primary_key=True)
#     dag = Column(String(250), primary_key=True)
#     count = Column(Integer())
#
#     __table_args__ = (
#         Index('loop_id', id, dag, unique=True),
#     )
