from propel import configuration
from propel.executors.celery_executor import CeleryExecutor

class ExecutorFactory(object):
    """
    Factory class that returns an instance of an executor
    """
    @staticmethod
    def get_executor():
        if configuration.get('core', 'executor'):
            return CeleryExecutor()
        else:
            return CeleryExecutor()
    