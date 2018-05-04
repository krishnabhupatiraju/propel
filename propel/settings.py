import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from propel import configuration

Engine = None
Session = None
logger = None


def configure_orm():
    global Session
    global Engine
    db = configuration.get('core', 'db')
    sql_alchemy_pool_size = int(configuration.get('core', 'sql_alchemy_pool_size'))
    logger.info(sql_alchemy_pool_size)
    Engine = create_engine(db, echo=False, pool_size=sql_alchemy_pool_size)
    Session = sessionmaker(bind=Engine)


def configure_logging():
    global logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)


configure_logging()
configure_orm()
