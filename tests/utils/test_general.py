import os
import pytest
import signal

from propel.utils.general import HeartbeatMixin


class TestHeartbeatMixin(object):

    @pytest.fixture
    def heartbeat_mixin_instance(self, monkeypatch):
        # Since we have no DB access mocking the method to update heartbeat model
        def mock__update_heartbeat(*args, **kwargs):
            return None

        monkeypatch.setattr(HeartbeatMixin, '_update_heartbeat', mock__update_heartbeat)
        return HeartbeatMixin()

    @pytest.fixture
    def function_with_exception(self):
        def func(*args, **kwargs):
            raise RuntimeError('Boink Boink')
        return func

    @pytest.fixture
    def function_write_something(self):
        def func(*args, **kwargs):
            print "Tring Tring with {} and {}".format(args, kwargs)
            import time
            # To simulate long task run
            time.sleep(30)
        return func

    @pytest.fixture
    def function_sysint(self):
        def func(*args, **kwargs):

            os.kill(os.getpid(), signal.SIGINT)
        return func

    def test_heartbeat_exception_in_thread(
            self,
            heartbeat_mixin_instance,
            function_with_exception
    ):
        with pytest.raises(RuntimeError, match="Boink Boink"):
            heartbeat_mixin_instance.heartbeat(thread_function=function_with_exception)

    def test_heartbeat_with_log_file(
            self,
            heartbeat_mixin_instance,
            function_write_something,
            tmpdir
    ):
        expected_output = "Tring Tring with (1, 2, 3) and {'a': 'a'}"
        tmp_file = tmpdir.join("tmp_file.txt").strpath
        heartbeat_mixin_instance.heartbeat(
            thread_function=function_write_something,
            thread_args=[1, 2, 3],
            thread_kwargs={'a': 'a'},
            log_file=tmp_file
        )
        found_text = False
        with open(tmp_file) as f:
            for line in f:
                if expected_output in line:
                    found_text = True
        assert found_text

    def test_heartbeat_child_process_receives_sysint(
            self,
            heartbeat_mixin_instance,
            function_sysint,
    ):
        with pytest.raises(SystemExit):
            heartbeat_mixin_instance.heartbeat(thread_function=function_sysint)
