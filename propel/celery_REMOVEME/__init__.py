from celery import Celery
from propel import configuration

broker = configuration.get('celery', 'broker')
app = Celery(broker=broker, backend='rpc://')
