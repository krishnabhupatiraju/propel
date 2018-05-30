import re
import signal
import sys
import time
import traceback
import threading
from datetime import datetime
from functools import wraps
from propel import configuration
from propel.settings import logger, Session


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


class HeartbeatMixin(object):
    """
    Mixin class that helps classes run a thread while the parent process produces a heartbeat
    """

    def heartbeat(self, thread_function, thread_args=None, thread_kwargs=None, logger=None):
        """
        Method that performs regularly sends a heartbeat while the thread is active

        :param thread_function: Function to run in a thread
        :type thread_function: function
        :param thread_args: Args to pass to the thread
        :type thread_args: list
        :param thread_kwargs: kwargs to pass to thread
        :type thread_kwargs: dict
        :param logger: logger to output logs to
        :type logger: logging.Logger
        """
        # If logger is None then use the default logger imported into the module
        if not logger:
            logger = globals()['logger']

        if not thread_args:
            thread_args = list()
        if not thread_kwargs:
            thread_kwargs = dict()

        def kill_process(signum, frame):
            logger.warning("Received signal {}. Exiting.".format(signum))
            sys.exit(0)

        # Handling interrupt signals
        signal.signal(signal.SIGTERM, kill_process)
        signal.signal(signal.SIGINT, kill_process)
        signal.signal(signal.SIGQUIT, kill_process)

        heartbeat_seconds = int(configuration.get('core', 'heartbeat_seconds'))
        thread = threading.Thread(
            target=thread_function,
            args=thread_args,
            kwargs=thread_kwargs
        )
        # Setting is as a daemon thread so the process exits when it received
        # a KILL signal. Otherwise sys.ext waits until the thread finishes
        thread.daemon = True
        thread.start()
        heartbeat = None
        session = Session()

        from propel.models import Heartbeats
        while thread.isAlive():
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
            time.sleep(heartbeat_seconds)
