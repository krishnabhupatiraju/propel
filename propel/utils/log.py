import logging
from contextlib import contextmanager
from propel import configuration

log_level = None
formatter = None
console_handler = None
logger = None


def configure_level():
    global log_level
    log_level_conf = configuration.get('log', 'level')
    if log_level_conf == 'CRITICAL':
        log_level = logging.CRITICAL
    elif log_level_conf == 'ERROR':
        log_level = logging.ERROR
    elif log_level_conf == 'WARNING':
        log_level = logging.WARNING
    elif log_level_conf == 'INFO':
        log_level = logging.INFO
    elif log_level_conf == 'DEBUG':
        log_level = logging.DEBUG
    else:
        log_level = logging.NOTSET


def configure_formatter():
    log_format = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
    global formatter
    formatter = logging.Formatter(log_format)


def configure_console_handler():
    global console_handler
    global log_level
    global formatter
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)


def configure_logger():
    global logger
    global console_handler
    global log_level
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    logger.addHandler(console_handler)
    logger.propagate = False


def __create_file_handler(log_filepath):
    global log_level
    global formatter
    file_handler = logging.FileHandler(filename=log_filepath)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    return file_handler


@contextmanager
def enable_file_handler_on_logger(log_filepath):
    global logger
    global console_handler
    file_handler = None
    try:
        file_handler = __create_file_handler(log_filepath)
        logger.addHandler(file_handler)
        logger.removeHandler(console_handler)
        yield
    finally:
        if file_handler in logger.handlers:
            logger.removeHandler(file_handler)
        if console_handler not in logger.handlers:
            logger.addHandler(console_handler)


configure_level()
configure_formatter()
configure_console_handler()
configure_logger()
