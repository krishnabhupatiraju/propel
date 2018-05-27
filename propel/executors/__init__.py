# This ideally should be a part of settings.py but running into
# circular imports so moved it to here
from propel import configuration

Executor = None


def configure_executor():
    global Executor
    executor_name = configuration.get('core', 'executor')
    if executor_name == 'Celery':
        from propel.executors.celery_executor import CeleryExecutor
        Executor = CeleryExecutor
    else:
        raise NotImplementedError('Executor type {} not defined'.format(executor_name))
    return Executor


configure_executor()
