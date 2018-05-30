class BaseTask(object):
    """
    Abstract BaseTask class that concrete subclasses will
    implement
    """
    def __init__(self, logger):
        self.logger = logger

    def execute(self, task):
        """
        Execute task

        :param task: An dict that contains details about the task to run
        :type task: dict
        """
        raise NotImplementedError()
