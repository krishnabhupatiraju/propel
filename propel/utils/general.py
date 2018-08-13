import billiard
import os
import re
import signal
import sys
import time
import threading
import traceback
from datetime import datetime
from functools import wraps
from propel import configuration
from propel.settings import logger, Session
from propel.utils.db import commit_db_object
from propel.utils.log import reset_logger


def _parse_operation(operation):
    """
    Given an operation string, method returns the name of the operation
    and a tuple of the parsed operations. Supported operations are
    access, filter and index

    access: Match any character that is not { or [

    filter: Must be enclosed between {}. Within braces should match format
    <string><operator><string>. Operator should match
    ==, !=, >, >=, <, <=

    index: Must be enclosed between []. Within brackets should match any
    digit or * character

    :param operation: Operation string
    :type operation: str
    """
    logger.info("Parsing operation: {}".format(operation))
    access_pattern = "^([^{[]+)$"
    filter_pattern = "^\{([^!=><]+)(\=\=|\!\=|\>|\>\=|\<|\<\=)([^!=><]+)\}$"
    index_pattern = "^\[(\d+|\*)\]$"
    access_regex = re.compile(access_pattern)
    filter_regex = re.compile(filter_pattern)
    index_regex = re.compile(index_pattern)
    # Matching operation string against the three operation types
    match_object = re.match(access_regex, operation)
    if match_object:
        logger.debug("Access operation")
        return 'access', match_object.groups()
    match_object = re.match(filter_regex, operation)
    if match_object:
        logger.debug("Filter operation")
        return 'filter', match_object.groups()
    match_object = re.match(index_regex, operation)
    if match_object:
        logger.debug("Index operation")
        return 'index', match_object.groups()
    logger.warn("Unknown operation type")
    return None, None


def extract_from_json(json_object, operations):
    """
    Perform operations on json and return the result.
    Operations is a sequential set of operation separated by a dot.
    Operation is one of 3 types - access, filter and index.

    Operation Types:
    Access operation is specified as a string and returns a matching
    object from dict

    Filter operation is specified in {<string><operator><string>} format.
    <operator> should match ==, !=, >, >=, <, <=. Operates on a dict and
    returns the dict as is if the filter condition is met

    Index operation is specified in  [<index>] format. <index> can be a
    number or *. When number is specified it returns the object at that
    index location. When star is specified it de-nests a list i.e. passes
    through each list item and returns the objects in the list.

    E.g. input_json = { 'name':  {'first':'Dilly', 'last':'Berty'},
                        'phone': [
                                 {'area':111, 'number':222333},
                                 {'area':444, 'number':555666},
                                 {'area':'aaa', 'number':'bbbccc'}
                                 ]
                        }
    extract_from_json(input_json, 'name')
    {'first': 'Dilly', 'last': 'Berty'}

    extract_from_json(input_json, 'name.first')
    'Dilly'

    extract_from_json(input_json, 'phone.{area=="aaa"}')
    None

    extract_from_json(input_json, 'phone.[*].{area=="aaa"}')
    {'area': 'aaa', 'number': 'bbbccc'}

    extract_from_json(input_json, 'phone.[*].{area!=111}')
    [{'number': 555666, 'area': 444}, {'number': 'bbbccc', 'area': 'aaa'}]

    extract_from_json(input_json, 'phone.[*].{area!=111}.[0]')
    {'number': 555666, 'area': 444}

    :param json_object: JSON Object
    :type json_object: Python object
    :param operations:Operation string
    :type operations: str
    """
    if not json_object:
        logger.warn("JSON data not provided")
        return None
    if not operations:
        logger.warn("Operations str not specified")
        return None
    stack = list()
    stack.append(json_object)
    try:
        for operation in operations.split('.'):
            stack_buffer = list()
            operation_type, operator = _parse_operation(operation)
            logger.debug("Processing JSON: {}".format(stack))
            if operation_type == 'access':
                logger.debug("Performing access operation")
                namespace = operator[0]
                for stack_item in stack:
                    stack_item_value = stack_item.get(namespace)
                    if stack_item_value is not None:
                        stack_buffer.append(stack_item_value)
            elif operation_type == 'filter':
                logger.debug("Performing filter operation")
                namespace, condition, value = operator
                for stack_item in stack:
                    filter_value = stack_item.get(namespace)
                    eval_string = "{}{}{}".format("filter_value",
                                                  condition,
                                                  value)
                    logger.debug(
                        "Evaluating {}{}{}".format(filter_value, condition, value)
                    )
                    if eval(eval_string):
                        stack_buffer.append(stack_item)
            elif operation_type == 'index':
                logger.debug("Performing index operation")
                index = operator[0]
                if index == "*":
                    logger.debug("Received * index. Stacking all entries within list")
                    for stack_item in stack:
                        if isinstance(stack_item, list):
                            for item in stack_item:
                                stack_buffer.append(item)
                elif index.isdigit():
                    stack_buffer.append(stack[int(index)])
            else:
                logger.warn("Unknown operation type. Cannot process further")
                return None
            stack = stack_buffer
            logger.debug("Processed JSON: {}".format(stack))
        # If result has a single item then return that item
        # else return the list of items
        if len(stack) == 0:
            return None
        if len(stack) == 1:
            return stack[0]
        else:
            return stack
    except Exception:
        logger.warn("Exception while processing json")
        traceback.print_exc()
        return None


def extract_multiple_from_json(json_object, operations_map):
    """
    Return a dictionary with name and output from performing corresponding
    operations on json_object. See extract_from_json
    documentation for more details

    E.g. extract_multiple_from_json(
    input_json,
    {'first_name': 'name.first', 'phone': 'phone.[*].[0]'}
    )
    {'first_name': 'Dilly', 'phone': {'area': 111, 'number': 222333}}

    :param json_object: JSON Object
    :type json_object: Python object
    :param operations_map: Mapping of name and operations.
    E.g. {'first_name': 'name.first', 'phone': 'phone.[*].[0]'}
    :param operations_map: dict
    """
    output = {}
    for name, operations in operations_map.iteritems():
        output[name] = extract_from_json(json_object, operations=operations)
    return output


class Memoize(object):
    """
    Decorator to memoize the results of the function until ttl.
    """

    def __init__(self, ttl):
        self.ttl = ttl
        self.expires_at = time.time() + ttl
        self.cache = dict()

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = str(args) + str(kwargs)
            if self.expires_at <= time.time():
                logger.debug('Cache expired for {}({},{})'.format(func, args, kwargs))
                self.expires_at = time.time() + self.ttl
                self.cache[key] = func(*args, **kwargs)
            elif key not in self.cache:
                logger.debug('Key not found in cache for {}({},{})'.format(func, args, kwargs))
                self.cache[key] = func(*args, **kwargs)
            else:
                logger.debug("Cache hit for {}({},{})".format(func, args, kwargs))
            return self.cache[key]

        return wrapper


class Singleton(type):
    """
    Intended to used as a __metaclass__ to enforce singleton pattern on a class
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class FunctionExceptionCatcher(object):
    """
    Wrapper around function that catches and records exceptions
    """

    def __init__(self, func):
        self.func = func
        self.exc_info = None

    def __call__(self, *args, **kwargs):
        try:
            self.func(*args, **kwargs)
        except BaseException:
            self.exc_info = sys.exc_info()


class HeartbeatMixin(object):
    """
    Mixin class that helps classes run a thread while the parent process produces a heartbeat
    """

    def heartbeat(
            self,
            thread_function,
            thread_args=None,
            thread_kwargs=None,
            log_file=None,
            heartbeat_model_kwargs=None
    ):
        """
        Method that runs process_function through "multiprocessing" and sends regular heartbeat
        If "process_log_file" is defined log output is redirected to process_log_file

        :param thread_function: Function to run as a thread with the heartbeat process
        :type thread_function: function
        :param thread_args: Args to pass to the thread
        :type thread_args: list
        :param thread_kwargs: kwargs to pass to thread
        :type thread_kwargs: dict
        :param log_file: File to which process output is logged
        :type log_file: str
        :param heartbeat_model_kwargs: kwargs that are passed when creating heartbeat entry
        :type heartbeat_model_kwargs: dict
        """

        def kill_heartbeat_process(signum, frame):
            logger.warning(
                "Received signal {}. Will kill heartbeat process with PID: {} and exit."
                .format(signum, heartbeat_process.pid, )
            )
            heartbeat_process.terminate()
            # Joining to ensure we wait for the child process to terminate
            heartbeat_process.join()
            # Exit from Python. This is implemented by raising the SystemExit exception,
            # so cleanup actions specified by finally clauses of try statements are honored,
            # and it is possible to intercept the exit attempt at an outer level.
            sys.exit(1)

        # Handling interrupt signals
        signal.signal(signal.SIGTERM, kill_heartbeat_process)
        signal.signal(signal.SIGINT, kill_heartbeat_process)
        signal.signal(signal.SIGQUIT, kill_heartbeat_process)

        # A queue to capture exceptions in child process
        child_process_exceptions = billiard.Queue()
        # Using billiard instead of multiprocessing since Celery creates a daemon process
        # to run a task and Python doesnt allow another daemon to be spun up from a
        # daemon process
        heartbeat_process = billiard.Process(
            target=self._heartbeat_with_logger_redirect,
            args=(
                thread_function,
                thread_args,
                thread_kwargs,
                log_file,
                heartbeat_model_kwargs,
                child_process_exceptions
            )
        )
        # # Setting is as a daemon process so the process exits when it receives
        # # a KILL signal. Otherwise sys.ext waits until the process finishes
        # heartbeat_process.daemon = True
        heartbeat_process.start()
        logger.info('Starting heartbeat process. PID: {}'.format(heartbeat_process.pid))
        heartbeat_process.join()

        # If exception occurred in child process then raise it in parent process
        if not child_process_exceptions.empty():
            (
                child_process_exception_type,
                child_process_exception_class,
                child_process_traceback
            ) = child_process_exceptions.get_nowait()
            raise child_process_exception_type(
                str(child_process_exception_class.message)
                + "\n"
                + child_process_traceback
            )

    def _heartbeat_with_logger_redirect(
            self,
            thread_function,
            thread_args=None,
            thread_kwargs=None,
            log_file=None,
            heartbeat_model_kwargs=None,
            exception_queue=None
    ):
        """
        Helper method that redirects output before calling the _run_heartbeat method

        :param thread_function: Function to run as a process
        :type thread_function: function
        :param thread_args: Args to pass to the process
        :type thread_args: list
        :param thread_kwargs: kwargs to pass to process
        :type thread_kwargs: dict
        :param log_file: File to which process output is logged
        :type log_file: str
        :param heartbeat_model_kwargs: kwargs that are passed when creating heartbeat entry
        :type heartbeat_model_kwargs: dict
        :param exception_queue: Queue to communicate exceptions in this process to parent
        :type exception_queue: billiard.Queue
        """
        try:
            if log_file:
                with open(log_file, 'a') as log_file_handle:
                    try:
                        logger.info("Redirecting output to {}".format(log_file))
                        sys.stdout = log_file_handle
                        sys.stderr = log_file_handle
                        # Resetting logger so it is configured to output to log_file
                        # See https://stackoverflow.com/questions/22105465/
                        # how-can-i-temporarily-redirect-the-output-of-logging-in-python
                        # /50652143#50652143
                        reset_logger()
                        self._run_heartbeat(
                            thread_function,
                            thread_args,
                            thread_kwargs,
                            heartbeat_model_kwargs
                        )
                    except Exception as e:
                        logger.exception(e)
                        raise
                    finally:
                        sys.stdout = sys.__stdout__
                        sys.stderr = sys.__stderr__
                        reset_logger()
            else:
                self._run_heartbeat(thread_function, thread_args, thread_kwargs)
        # Catch every exception including SystemExit and KeyboardInterrupt
        # Note: SystemExit is not captured by Exception.
        except BaseException:
            # If exception queue is specified add exception to Queue.
            if exception_queue:
                exception_type, exception_class, tb = sys.exc_info()
                # traceback info is not pickleable. So extracting it as str before adding to queue
                exception_queue.put((exception_type, exception_class, traceback.format_exc(tb)))
                exception_queue.close()
            raise

    def _run_heartbeat(
            self,
            thread_function,
            thread_args=None,
            thread_kwargs=None,
            heartbeat_model_kwargs=None
    ):
        """
        Helper method that runs thread_function through "threading" and sends regular heartbeat

        :param thread_function: Function to run as a thread
        :type thread_function: function
        :param thread_args: Args to pass to the thread
        :type thread_args: list
        :param thread_kwargs: kwargs to pass to thread
        :type thread_kwargs: dict
        :param heartbeat_model_kwargs: kwargs that are passed when creating heartbeat entry
        :type heartbeat_model_kwargs: dict
        """

        def poison_pill(signum, frame):
            logger.warning("Received signal {}. Taking poison pill".format(signum))
            # Exit from Python. This is implemented by raising the SystemExit exception,
            # so cleanup actions specified by finally clauses of try statements are honored,
            # and it is possible to intercept the exit attempt at an outer level. Note that
            # SystemExit is not caught by except Exception
            sys.exit(1)

        # Handling interrupt signals
        signal.signal(signal.SIGTERM, poison_pill)
        signal.signal(signal.SIGINT, poison_pill)
        signal.signal(signal.SIGQUIT, poison_pill)

        if not thread_args:
            thread_args = list()
        if not thread_kwargs:
            thread_kwargs = dict()

        # Wrapping thread function within an exception handler so we know if the thread
        # failed with an exception
        thread_function_with_exception_catcher = FunctionExceptionCatcher(func=thread_function)

        # Using billiard instead of multiprocessing since Celery creates a daemon process
        # to run a task and Python doesnt allow another daemon to be spun up from that process
        thread = threading.Thread(
            target=thread_function_with_exception_catcher,
            args=thread_args,
            kwargs=thread_kwargs
        )
        # Setting is as a daemon thread so the process can exit when it receives
        # a KILL signal. Otherwise sys.ext waits for the thread to finish.
        thread.daemon = True
        thread.start()
        heartbeat=None
        heartbeat_seconds = int(configuration.get('core', 'heartbeat_seconds'))
        while thread.isAlive():
            if not heartbeat:
                heartbeat = self._update_heartbeat(
                    heartbeat=None,
                    heartbeat_model_kwargs=heartbeat_model_kwargs
                )
            else:
                self._update_heartbeat(
                    heartbeat=heartbeat,
                    heartbeat_model_kwargs=heartbeat_model_kwargs
                )
            logger.info('Woot Woot from PID: {}'.format(os.getpid()))
            time.sleep(heartbeat_seconds)

        thread_exc_info = thread_function_with_exception_catcher.exc_info
        if thread_exc_info:
            # If exception occurred in thread then raise it in main thread
            raise thread_exc_info[1], None, thread_exc_info[2]

    def _update_heartbeat(self, heartbeat, heartbeat_model_kwargs):
        from propel.models import Heartbeats
        if not heartbeat:
            heartbeat = Heartbeats(
                task_type=self.__class__.__name__,
                last_heartbeat_time=datetime.utcnow(),
                **heartbeat_model_kwargs if heartbeat_model_kwargs else dict()
            )
        else:
            heartbeat.last_heartbeat_time = datetime.utcnow()
        commit_db_object(heartbeat)
        return heartbeat


