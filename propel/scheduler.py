from datetime import datetime, timedelta
from sqlalchemy import func

from propel.models import Tasks, TaskRuns
from propel.settings import logger
from propel.utils.db import provide_session
from propel.utils.general import Memoize
from propel.utils.state import State


class Scheduler(object):

    @staticmethod
    def run():
        while True:
            current_datetime = datetime.utcnow()
            tasks_to_run = Scheduler._get_eligible_tasks_to_run(current_datetime)
            for task_to_run in tasks_to_run:
                Scheduler._insert_new_task_run_to_db(
                    task_id=task_to_run[0].id,
                    state=State.QUEUED,
                    run_ds=task_to_run[1]
                )
            import time
            time.sleep(5)

    @staticmethod
    @provide_session
    def _insert_new_task_run_to_db(session=None, **kwargs):
        task = TaskRuns(**kwargs)
        session.add(task)
        session.commit()

    @staticmethod
    def _get_eligible_tasks_to_run(current_datetime):
        eligible_tasks_to_run = []
        tasks = Scheduler._get_tasks()
        last_task_runs = Scheduler._get_last_task_runs()
        for task in tasks:
            task_last_run_ds = last_task_runs.get(task.id)
            if task_last_run_ds:
                next_run_ds = task_last_run_ds + timedelta(seconds=task.run_frequency_seconds)
                if next_run_ds <= current_datetime:
                    logger.debug(
                        "Scheduling Task {} for {}"
                            .format(task.task_name, next_run_ds)
                    )
                    eligible_tasks_to_run.append((task,next_run_ds))
                else:
                    logger.debug(
                        "Task {} still not eligible to run for {}"
                        .format(task.task_name, next_run_ds)
                    )
            else:
                next_run_ds = (current_datetime -
                               timedelta(minutes=round(current_datetime.minute, -1),
                                         seconds=current_datetime.second,
                                         microseconds=current_datetime.microsecond
                                         )
                               )
                logger.debug(
                    "Task {} never ran. Scheduling it for {}"
                    .format(task.task_name, next_run_ds)
                )
                eligible_tasks_to_run.append((task, next_run_ds))
        return eligible_tasks_to_run

    @staticmethod
    @provide_session
    def _get_last_task_runs(session=None):
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

    @staticmethod
    @Memoize(ttl=30)
    @provide_session
    def _get_tasks(session=None):
        return session.query(Tasks).all()


if __name__ == '__main__':
        # Scheduler.run()
        print Scheduler.run()
