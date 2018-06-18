from propel import configuration
from propel.settings import logger
from propel.utils.general import HeartbeatMixin


class BaseExecutor(HeartbeatMixin):

    def _get_task_class_factory(self, task_run_params):
        task_type = task_run_params['task_type']
        if task_type == 'TwitterExtract':
            from propel.tasks.twitter_extract import TwitterExtract
            task_class = TwitterExtract
        else:
            raise NotImplementedError('Task type {} not defined'.format(task_type))
        return task_class

    def execute(self, task_run_params):
        logger.info('Running TaskRun {}'.format(task_run_params))
        task_run_id = task_run_params['task_run_id']
        log_file = (
                configuration.get('log', 'tasks_log_location')
                + str(task_run_id)
                + '.log'
        )
        task_class = self._get_task_class_factory(task_run_params)
        self.heartbeat(
            thread_function=task_class().execute,
            thread_args=[task_run_params],
            log_file=log_file,
            heartbeat_model_kwargs={'task_run_id':task_run_id}
        )

    def execute_async(self, task_run_params):
        return NotImplementedError()

    def start(self, concurrency):
        return NotImplementedError()
