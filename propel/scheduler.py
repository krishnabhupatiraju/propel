import time
import threading
from datetime import datetime, timedelta
from sqlalchemy import func
from propel import configuration
from propel.models import Tasks, TaskRuns, Heartbeats
from propel.settings import logger, Session, Executor
from propel.utils.db import provide_session
from propel.utils.general import Memoize
from propel.utils.state import State


class Scheduler(object):

    @staticmethod
    @Memoize(ttl=300)
    @provide_session
    def _get_tasks(session=None):
        return session.query(Tasks).all()

    @provide_session
    def _get_last_task_runs(self, session=None):
        last_task_runs = (
            session.query(
                TaskRuns.task_id.label('task_id'),
                func.max(TaskRuns.run_ds).label('max_run_ds')
            ).group_by(TaskRuns.task_id)
        )
        return {
            last_task_run.task_id: last_task_run.max_run_ds
            for last_task_run in last_task_runs
        }

    def _get_eligible_tasks_to_run(self, current_datetime):
        eligible_tasks_to_run = []
        tasks = self._get_tasks()
        last_task_runs = self._get_last_task_runs()
        for task in tasks:
            task_last_run_ds = last_task_runs.get(task.id)
            if task_last_run_ds:
                next_run_ds = task_last_run_ds + timedelta(seconds=task.run_frequency_seconds)
                if next_run_ds <= current_datetime:
                    logger.debug(
                        "Scheduling Task {} for {}"
                        .format(task.task_name, next_run_ds)
                    )
                    task.run_ds = next_run_ds
                    eligible_tasks_to_run.append(task)
                else:
                    logger.debug(
                        "Task {} still not eligible to run for {}"
                        .format(task.task_name, next_run_ds)
                    )
            else:
                next_run_ds = (current_datetime -
                               timedelta(seconds=round(current_datetime.second, -1),
                                         microseconds=current_datetime.microsecond
                                         )
                               )
                logger.debug(
                    "Task {} never ran. Scheduling it for {}"
                    .format(task.task_name, next_run_ds)
                )
                task.run_ds = next_run_ds
                eligible_tasks_to_run.append(task)
        return eligible_tasks_to_run

    @provide_session
    def _insert_new_task_run_to_db(self, session=None, **kwargs):
        task = TaskRuns(**kwargs)
        session.add(task)
        session.commit()

    def _schedule_tasks(self):
        scheduler_sleep_seconds = int(configuration.get('core', 'scheduler_sleep_seconds'))
        executor = Executor()
        while True:
            current_datetime = datetime.utcnow()
            tasks_to_run = self._get_eligible_tasks_to_run(current_datetime)
            for task_to_run in tasks_to_run:
                self._insert_new_task_run_to_db(
                    task_id=task_to_run.id,
                    state=State.QUEUED,
                    run_ds=task_to_run.run_ds
                )
                executor.execute_async(task_to_run)
            time.sleep(scheduler_sleep_seconds)

    def run(self):
        self.heartbeat(thread_function=self._schedule_tasks)

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


if __name__ == '__main__':
        # Scheduler.run()
        print Scheduler().run()
