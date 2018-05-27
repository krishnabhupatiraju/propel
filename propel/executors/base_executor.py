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
        task_class = self._get_task_class_factory(task)
        self.heartbeat(thread_function=task_class().execute, task=task)

    def execute_async(self, task):
        return NotImplementedError()

    def start(self, concurrency):
        return NotImplementedError()
