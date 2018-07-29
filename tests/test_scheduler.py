from datetime import datetime, timedelta
from propel.scheduler import Scheduler


class TasksMock(object):
    def __init__(self, id, task_name, schedule_latest, run_frequency_seconds):
        self.id = id
        self.task_name = task_name
        self.schedule_latest = schedule_latest
        self.run_frequency_seconds = run_frequency_seconds

    def as_dict(self):
        return self.__dict__


class TestScheduler(object):
    def test__get_eligible_tasks_to_run(self, monkeypatch):

        def _get_tasks_mock():
            # Task 1: Testing first time run
            # Task 2: Past Run without schedule latest
            # Task 3: Past Run with schedule latest. Past run was long back
            # Task 4: Past Run with schedule latest. Past run was recent
            task1 = TasksMock(1, 'task1', True, 60)
            task2 = TasksMock(2, 'task2', False, 60)
            task3 = TasksMock(3, 'task3', True, 60)
            task4 = TasksMock(4, 'task4', True, 60)
            return [task1, task2, task3, task4]

        def _get_last_task_runs_mock():
            return {
                2: datetime(2018, 6, 28, 0, 0, 0),
                3: datetime(2018, 6, 28, 0, 0, 0),
                4: datetime(2018, 7, 27, 23, 59, 59),
            }

        scheduler = Scheduler()
        monkeypatch.setattr(scheduler, '_get_tasks', _get_tasks_mock)
        monkeypatch.setattr(scheduler, '_get_last_task_runs', _get_last_task_runs_mock)

        mocked_current_datetime=datetime(2018, 7, 28, 0, 0, 0)
        returned_eligible_tasks_to_run = scheduler._get_eligible_tasks_to_run(
            current_datetime=mocked_current_datetime
        )
        expected_eligible_tasks_to_run = [
            {
                'id': 1,
                'task_name': 'task1',
                'schedule_latest': True,
                'run_frequency_seconds': 60,
                'run_ds': mocked_current_datetime,
                'interval_start_ds': mocked_current_datetime-timedelta(seconds=60),
                'interval_end_ds': mocked_current_datetime,
            },
            {
                'id': 2,
                'task_name': 'task2',
                'schedule_latest': False,
                'run_frequency_seconds': 60,
                'run_ds': datetime(2018, 6, 28, 0, 0, 0)+timedelta(seconds=60),
                'interval_start_ds': datetime(2018, 6, 28, 0, 0, 0),
                'interval_end_ds': datetime(2018, 6, 28, 0, 0, 0)+timedelta(seconds=60),
            },
            {
                'id': 3,
                'task_name': 'task3',
                'schedule_latest': True,
                'run_frequency_seconds': 60,
                'run_ds': mocked_current_datetime,
                'interval_start_ds': mocked_current_datetime-timedelta(seconds=60),
                'interval_end_ds': mocked_current_datetime,
            },
        ]
        assert (
                sorted(returned_eligible_tasks_to_run, key=lambda x: x['id'])
                == sorted(expected_eligible_tasks_to_run, key=lambda x: x['id'])
        )
