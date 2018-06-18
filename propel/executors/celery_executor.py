import subprocess
from celery import Celery
from propel import configuration
from propel.executors.base_executor import BaseExecutor
from propel.settings import logger


celery_broker = configuration.get('celery', 'broker')
celery_app = Celery(__name__, broker=celery_broker)


@celery_app.task
def execute_celery_task(task_run_params):
    BaseExecutor().execute(task_run_params)


class CeleryExecutor(BaseExecutor):

    def execute_async(self, task_run_params):
        logger.info('Adding TaskRun {} to celery queue'.format(task_run_params))
        execute_celery_task.apply_async(args=[task_run_params, ])
        logger.debug('TaskRun {} should run soon'.format(task_run_params))

    def start(self, concurrency):
        start_worker_cmd = [
            "celery",
            "-A",
            "{}".format(__name__),
            "worker",
            "--concurrency={}".format(concurrency)
            ]
        # Not setting stdin, stdout and stderr so these will be output
        # to the calling terminal
        executor_process = subprocess.Popen(
            args=start_worker_cmd
        )
        logger.info(
            'Started {} PID: {}'
            .format(self.__class__.__name__, executor_process.pid)
        )
        executor_process.communicate()
        return executor_process.pid
