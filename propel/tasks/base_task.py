class BaseTask(object):
    """
    Abstract BaseTask class that concrete subclasses will
    implement
    """

    def execute(self, task):
        """
        Execute task

        :param task: An dict that contains details about the task to run
        :type task: dict
        """
        raise NotImplementedError()
