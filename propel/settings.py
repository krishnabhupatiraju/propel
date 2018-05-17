import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from propel import configuration

SIMPLE_LOG_FORMAT = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
Engine = None
Session = None
logger = None


def configure_orm():
    global Session
    global Engine
    db = configuration.get('core', 'db')
    sql_alchemy_pool_size = int(configuration.get('core', 'sql_alchemy_pool_size'))
    Engine = create_engine(db, echo=False, pool_size=sql_alchemy_pool_size)
    Session = sessionmaker(bind=Engine)


def configure_logging():
    global logger
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
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    formatter = logging.Formatter(SIMPLE_LOG_FORMAT)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.propagate = False


configure_logging()
configure_orm()
