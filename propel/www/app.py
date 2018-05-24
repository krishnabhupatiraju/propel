from flask import Flask
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView

from propel import configuration

from propel.models import Connections, TaskGroups, Tasks, TaskRuns, Heartbeats, Tweets
from propel.settings import Session


def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = configuration.get('flask', 'secret')
    admin = Admin(app, name='propel', template_mode='bootstrap3')
    admin.add_view(ModelView(Connections, Session))
    admin.add_view(ModelView(TaskGroups, Session))
    admin.add_view(ModelView(Tasks, Session))
    admin.add_view(ModelView(TaskRuns, Session))
    admin.add_view(ModelView(Heartbeats, Session))
    admin.add_view(ModelView(Tweets, Session))

    # After the request response cycle is complete removing the scoped session.
    # Otherwise data added to the DB but external processes (like scheduler or worker )
    # are not viewable through the UI due to REPEATABLE_READ isolation in MySql.
    @app.teardown_appcontext
    def shutdown_session(response_or_exc):
        Session.remove()
        return response_or_exc

    return app
