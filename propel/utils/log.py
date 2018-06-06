import logging
import sys
from propel import configuration

log_level = None
formatter = None
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


def get_console_handler():
    global log_level
    global formatter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    return console_handler


def configure_logger():
    global logger
    global log_level
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    logger.addHandler(get_console_handler())
    logger.propagate = False


def reset_logger():
    """
    Reset logger is used when trying to redirect logged output
    """
    global logger
    # Making a list so we dont mutate the list we are iterating on
    handlers = list(logger.handlers)
    for handler in handlers:
        logger.removeHandler(handler)
    configure_logger()


configure_level()
configure_formatter()
configure_logger()
