from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

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
    # Using a scoped session so we can create a session per request-response cycle.
    # See www/app.py where a session is removed after every request.
    Session = scoped_session(sessionmaker(bind=Engine))


def configure_logging():
    global logger
    from propel.utils.log import logger as _logger
    logger = _logger


configure_logging()
configure_orm()
