from propel import configuration
from propel.settings import logger
from propel.utils.general import HeartbeatMixin
from propel.utils.log import create_file_logger, redirect_stderr, redirect_stdout


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

        file_logger = create_file_logger(
            caller_name=self.__class__.__module__ + '.' + self.__class__.__name__,
            log_filepath=(
                    configuration.get('log', 'tasks_log_location')
                    + str(task['task_run_id'])
                    + '.log'
            )
        )
        task_class = self._get_task_class_factory(task)
        self.heartbeat(
            process_function=task_class().execute,
            process_args=[task],
            process_logger=file_logger
        )

    def execute_async(self, task):
        return NotImplementedError()

    def start(self, concurrency):
        return NotImplementedError()
