from flask import Flask, Markup
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView

from propel import configuration

from propel.models import Connections, TaskGroups, Tasks, TaskRuns, Heartbeats, Tweets
from propel.settings import Session


class TweetsView(ModelView):
    page_size = 50
    column_exclude_list = ['raw_tweet', ]
    can_export = True
    column_filters = [
        'tweet_created_at',
        'user_screen_name',
        'tweet_type'
    ]
    column_list = [
        'tweet_id',
        'tweet_created_at',
        'user_screen_name',
        'tweet_type',
        'text',
        'favorite_count',
        'retweet_count',
        'quoted_text',
        'expanded_urls',
        'media_urls',
        'media_types'
    ]
    # Adding rest of the columns to the end of the columns list
    column_list.extend(
        [
            column.name
            for column in Tweets.__table__.columns._all_columns
            if column.name not in column_list
        ]
    )
    can_create = False

    def create_anchor_link(view, context, model, name):
        return Markup(
            "<a href='https://twitter.com/i/web/status/{}' target='_blank'>Link</a>"
            .format(getattr(model, name))
        )

    column_formatters = dict(tweet_id=create_anchor_link)
    # list_template = "tweets.html"


def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = configuration.get('flask', 'secret')
    admin = Admin(app, name='propel', template_mode='bootstrap3')
    admin.add_view(ModelView(Connections, Session))
    admin.add_view(ModelView(TaskGroups, Session))
    admin.add_view(ModelView(Tasks, Session))
    admin.add_view(ModelView(TaskRuns, Session))
    admin.add_view(ModelView(Heartbeats, Session))
    admin.add_view(TweetsView(Tweets, Session))

    # After the request response cycle is complete removing the scoped session.
    # Otherwise data added to the DB but external processes (like scheduler or worker )
    # are not viewable through the UI due to REPEATABLE_READ isolation in MySql.
    @app.teardown_appcontext
    def shutdown_session(response_or_exc):
        Session.remove()
        return response_or_exc

    return app
