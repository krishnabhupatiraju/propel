import inspect

from contextlib import contextmanager
from functools import wraps

from propel.settings import Session


@contextmanager
def create_session():
    """
    Context manager that provides a session
    """
    session = Session()
    try:
        yield session
        session.expunge_all()
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def provide_session(func):
    """
    Decorator that provides a session to the calling function if one is
    not provided as function argument. Session Commit, Rollback and
    Close are handled automatically
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        session_arg = 'session'
        func_args = inspect.getargspec(func).args
        # Check if session is passed as part of kwargs or args
        session_in_kwargs = session_arg in kwargs
        session_in_args = (
                session_arg in func_args
                and len(args) > func_args.index(session_arg)
        )
        if session_in_kwargs or session_in_args:
            return func(*args, **kwargs)
        else:
            with create_session() as session:
                kwargs[session_arg] = session
                return func(*args, **kwargs)
    return wrapper


@provide_session
def commit_db_object(db_object, session=None):
    session.add(db_object)
    session.commit()
