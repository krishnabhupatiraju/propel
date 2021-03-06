import time
from datetime import datetime, timedelta
from sqlalchemy import func
from propel import configuration
from propel.models import Tasks, TaskRuns
from propel.settings import logger
from propel.executors import Executor
from propel.utils.db import provide_session
from propel.utils.general import Memoize, HeartbeatMixin
from propel.utils.state import State


class Scheduler(HeartbeatMixin):

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
            task_run_params = task.as_dict()
            is_first_time_run = False if task_last_run_ds else True
            if is_first_time_run:
                # If task is running for first time, run it for the
                # nearest minute before current_datetime
                next_run_ds = (
                        current_datetime -
                        timedelta(
                            seconds=current_datetime.second,
                            microseconds=current_datetime.microsecond
                        )
                )
                logger.debug(
                    "Task {} never ran. Will schedule it for {}"
                    .format(task.task_name, next_run_ds)
                )
            else:
                next_run_ds = task_last_run_ds + timedelta(seconds=task.run_frequency_seconds)
                # If schedule_latest then skip intermediate runs. This is useful when
                # the scheduler is down for long periods of time. In that case this
                # setting will make the scheduler schedule only the latest run instead
                # of catching up
                if task.schedule_latest:
                    catchup_seconds = int(
                        (current_datetime - task_last_run_ds).total_seconds() /
                        task.run_frequency_seconds
                    ) * task.run_frequency_seconds
                    latest_possible_run = task_last_run_ds + timedelta(seconds=catchup_seconds)
                    next_run_ds = max(next_run_ds, latest_possible_run)
            if next_run_ds <= current_datetime:
                logger.debug(
                    "Scheduling Task {} for {}".format(task.task_name, next_run_ds)
                )
                task_run_params['run_ds'] = next_run_ds
                task_run_params['interval_start_ds'] = (
                        next_run_ds
                        - timedelta(seconds=task.run_frequency_seconds)
                )
                task_run_params['interval_end_ds'] = next_run_ds
                eligible_tasks_to_run.append(task_run_params)
            else:
                logger.debug(
                    "Task {} still not eligible to run for {}".format(task.task_name, next_run_ds)
                )
        return eligible_tasks_to_run

    @provide_session
    def _insert_new_task_run_to_db(self, session=None, **kwargs):
        task_run = TaskRuns(**kwargs)
        session.add(task_run)
        session.commit()
        return task_run.id

    def _schedule_tasks(self):
        scheduler_sleep_seconds = int(configuration.get('core', 'scheduler_sleep_seconds'))
        executor = Executor()
        while True:
            current_datetime = datetime.utcnow()
            # Parse DAGs



            tasks_to_run = self._get_eligible_tasks_to_run(current_datetime)
            for task_run_params in tasks_to_run:
                task_run_id = self._insert_new_task_run_to_db(
                    task_id=task_run_params['id'],
                    state=State.QUEUED,
                    run_ds=task_run_params['run_ds']
                )
                task_run_params['task_run_id'] = task_run_id
                executor.execute_async(task_run_params)
            logger.debug(
                'Sleeping for {} seconds before trying to schedule again'
                .format(scheduler_sleep_seconds)
            )
            time.sleep(scheduler_sleep_seconds)

    def run(self):
        self.heartbeat(thread_function=self._schedule_tasks)
