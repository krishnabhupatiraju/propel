import json
import logging

from celery import Celery
from propel import configuration
from propel.executors.base_executor import BaseExecutor 

broker = configuration.get('celery', 'broker')
app = Celery(broker=broker, backend='rpc://')

@app.task(bind=True,
          autoretry_for=(Exception,),
          max_retries=3,
          default_retry_delay=180,
          retry_backoff=60,
          soft_time_limit=600,
          time_limit=720,
          acks_late=False
          )
def execute_command(self, command):
    print command
    logging.info('Celery Worker executing task:{} args:{} kwargs:{}'
                 .format(self.request.id,
                         self.request.args,
                         self.request.kwargs))

class CeleryExecutor(BaseExecutor): 
    
    def __init__(self):
        pass
    
    def execute(self, command):
        """
        Execute 'command' through a Celery Worker
        """
        print 'Queuing command {} to Celery'.format(command)
        logging.info('Queuing command {} to Celery'.format(command))
        return execute_command.apply_async(args=[command])