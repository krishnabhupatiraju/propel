from propel import configuration
from propel.models import TaskRuns
from propel.settings import logger
from propel.utils.db import commit_db_object
from propel.utils.general import HeartbeatMixin
from propel.utils.state import State


class BaseExecutor(HeartbeatMixin):

    def _get_task_class_factory(self, task_run_params):
        task_type = task_run_params['task_type']
        if task_type == 'TwitterExtract':
            from propel.tasks.twitter_extract import TwitterExtract
            task_class = TwitterExtract
        elif task_type == 'NewsDownload':
            from propel.tasks.news_download import NewsDownload
            task_class = NewsDownload
        else:
            raise NotImplementedError('Task type {} not defined'.format(task_type))
        return task_class

    def execute(self, task_run_params, session=None):
        logger.info('Running TaskRun {}'.format(task_run_params))
        task_run_id = task_run_params['task_run_id']
        task_run = TaskRuns(task_run_id=task_run_id)
        log_file = (
                configuration.get('log', 'tasks_log_location')
                + str(task_run_id)
                + '.log'
        )
        task_class = self._get_task_class_factory(task_run_params)
        try:
            task_run.state = State.RUNNING
            commit_db_object(task_run)
            self.heartbeat(
                thread_function=task_class().execute,
                thread_args=[task_run_params],
                log_file=log_file,
                heartbeat_model_kwargs={'task_run_id': task_run_id}
            )
        # Update task status to Error for any exception including KeyboardInterrupt and SystemExit
        except:
            task_run.state = State.FAILED
            commit_db_object(task_run)
        else:
            task_run.state = State.SUCCESS
            commit_db_object(task_run)

    def execute_async(self, task_run_params):
        return NotImplementedError()

    def start(self, concurrency):
        return NotImplementedError()
