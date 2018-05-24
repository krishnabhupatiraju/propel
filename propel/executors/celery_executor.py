from celery import Celery

from propel import configuration
from propel.executors.base_executor import BaseExecutor
from propel.settings import logger


celery_broker = configuration.get('celery', 'broker')
celery_app = Celery(__name__, broker=celery_broker)


class CeleryExecutor(BaseExecutor):

    def execute_async(self, task):

        @celery_app.task
        def execute(task):
            self.execute(task)

        logger.info('Adding task {} to celery queue'.format(task))
        execute.apply_async(args=(task,))
        logger.debug('Task {} should run soon'.format(task))
