from collections import deque
import signal

from propel.utils.general import Singleton


class WorkerManager():
    __metaclass__ = Singleton

    def __init__(self):
        self.queue = deque()
        signal.signal(signal.SIGTERM, )

    def add_task_to_queue(self, task):
        """
        Interface to the outside world. A task is added through this method.
        Once in queue the worker takes care of executing the task

        :param task: A task that needs to be executed
        :type task: instance of propel.models.Tasks
        """
        self.queue.append(task)

    def assign_tasks_to_workers(self):
        while

    def start(self):



