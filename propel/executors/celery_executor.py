import subprocess
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

    def start(self, concurrency):
        start_worker = [
            "celery",
            "-A",
            "{}".format(__name__),
            "concurrency",
            "{}".format(concurrency)
            ]
        executor_process = subprocess.Popen(
            args=start_worker
        )
        logger.info(
            'Started {} PID: {}'
            .format(self.__class__.__name__, executor_process.pid)
        )
        # Not setting stdin, stdout and stderr so these will be output
        # to the calling terminal
        executor_process.communicate()
        return executor_process.pid
