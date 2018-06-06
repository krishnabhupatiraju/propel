from propel import configuration
from propel.settings import logger
from propel.utils.general import HeartbeatMixin


class BaseExecutor(HeartbeatMixin):

    def _get_task_class_factory(self, task):
        task_type = task['task_type']
        if task_type == 'TwitterExtract':
            from propel.tasks.twitter_extract import TwitterExtract
            task = TwitterExtract
        else:
            raise NotImplementedError('Task type {} not defined'.format(task_type))
        return task

    def execute(self, task):
        logger.info('Running Task {}'.format(task))
        process_log_file = (
                configuration.get('log', 'tasks_log_location')
                + str(task['task_run_id'])
                + '.log'
        )
        task_class = self._get_task_class_factory(task)
        self.heartbeat(
            thread_function=task_class().execute,
            thread_args=[task],
            log_file=process_log_file
        )

    def execute_async(self, task):
        return NotImplementedError()

    def start(self, concurrency):
        return NotImplementedError()
