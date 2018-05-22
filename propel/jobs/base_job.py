import threading
import time

from datetime import datetime

from propel import configuration
from propel.models import Heartbeats
from propel.settings import logger, Session


class BaseJob(object):

    def heartbeat(self, thread_function, *thread_args, **thread_kwargs):
        heartbeat_seconds = int(configuration.get('core', 'heartbeat_seconds'))
        thread = threading.Thread(
            target=thread_function,
            args=thread_args,
            kwargs=thread_kwargs
        )
        thread.start()
        heartbeat = None
        session = Session()

        while thread.isAlive():
            time.sleep(heartbeat_seconds)
            if not heartbeat:
                heartbeat = Heartbeats(
                    task_type=self.__class__.__name__,
                    last_heartbeat_time=datetime.utcnow()
                )
            else:
                heartbeat.last_heartbeat_time = datetime.utcnow()
            session.add(heartbeat)
            session.commit()
            logger.info('Woot Woot')

    def run(self):
        return NotImplementedError
