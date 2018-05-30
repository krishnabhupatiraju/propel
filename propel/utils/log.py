import logging
import sys
from contextlib import contextmanager
from propel.settings import logger, SIMPLE_LOG_FORMAT


def create_file_logger(caller_name, log_filepath):
    file_logger = logger.getChild(caller_name)
    file_logger.setLevel(logger.level)
    file_handler = logging.FileHandler(filename=log_filepath)
    file_handler.setLevel(logger.level)
    formatter = logging.Formatter(SIMPLE_LOG_FORMAT)
    file_handler.setFormatter(formatter)
    file_logger.addHandler(file_handler)
    file_logger.propagate = False
    return file_logger


class RedirectHandler(object):
    """
    Provide a file like interface so sysout and stderr can be redirected to logger
    """
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
        self._buffer = str()

    def write(self, message):
        self._buffer += message
        if message.endswith("\n"):
            self.logger.log(self.level, self._buffer)
            self._buffer = str()

    def flush(self):
        if len(self._buffer) > 0:
            self.logger.log(self.level, self._buffer)
            self._buffer = str()


@contextmanager
def redirect_stderr(to_logger, as_level):
    try:
        sys.stderr = RedirectHandler(logger=to_logger, level=as_level)
        yield
    finally:
        sys.stderr = sys.__stderr__


@contextmanager
def redirect_stdout(to_logger, as_level):
    try:
        sys.stdout = RedirectHandler(logger=to_logger, level=as_level)
        yield
    finally:
        sys.stdout = sys.__stdout__
