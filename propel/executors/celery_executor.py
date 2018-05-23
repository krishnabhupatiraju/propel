from propel.executors.base_executor import BaseExecutor
from propel.settings import logger


class CeleryExecutor(BaseExecutor):

    def execute_async(self, task):
        logger.info('Adding task {} to celery queue'.format(task))