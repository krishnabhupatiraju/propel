from celery import Celery
app = Celery('propel', broker='amqp://localhost')