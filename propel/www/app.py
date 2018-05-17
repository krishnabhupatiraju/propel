from propel import configuration, models
from propel.settings import Session

from flask import Flask
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView


def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = configuration.get('flask', 'secret')
    admin = Admin(app, name='propel', template_mode='bootstrap3')
    session = Session()
    admin.add_view(ModelView(models.Connections, session))
    admin.add_view(ModelView(models.TaskGroups, session))
    admin.add_view(ModelView(models.Tasks, session))
    admin.add_view(ModelView(models.TaskRuns, session))
    admin.add_view(ModelView(models.Tweets, session))
    return app
