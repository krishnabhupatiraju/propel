from collections import defaultdict
from flask import Flask, Markup
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from flask_admin import BaseView, expose

from propel import configuration

from propel.models import Connections, TaskGroups, Tasks, TaskRuns, Heartbeats, Tweets
from propel.settings import Session
from propel.utils.db import provide_session
from propel.www import forms


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


class TaskRunsView(ModelView):
    column_list = [
            column.name
            for column in TaskRuns.__table__.columns._all_columns
        ]


class TweetsDeckView(BaseView):
    @expose('/', methods=['GET', 'POST'])
    @provide_session
    def index(self, session=None):
        form = forms.TweetsDeck()
        tweets = defaultdict(list)
        if form.validate_on_submit():
            user_screen_names = map(lambda x: x.strip(), form.user_screen_names.data.split(','))
            tweets_start_dt = form.tweets_start_dt.data
            tweets_end_dt = form.tweets_end_dt.data
            matching_tweets = (
                session
                .query(Tweets)
                .filter(Tweets.user_screen_name.in_(user_screen_names))
                .filter(Tweets.tweet_created_at >= tweets_start_dt)
                .filter(Tweets.tweet_created_at <= tweets_end_dt)
                .all()
            )
            for tweet in matching_tweets:
                tweet_attributes = list()
                tweet_attributes.append(tweet.tweet_type)
                tweet_attributes.append(tweet.get_full_text())
                tweet_attributes.append(tweet.favorite_count)
                tweet_attributes.append(tweet.tweet_id)
                tweets[tweet.user_screen_name].append(tweet_attributes)
            # Sorting tweets based on favorite_count
            for user_screen_name, tweet_attributes in tweets.items():
                tweets[user_screen_name] = sorted(
                    tweet_attributes,
                    key=lambda x: int(x[2]),
                    reverse=True
                )
        return self.render('tweets_deck.html', tweets=tweets, form=form)


def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = configuration.get('flask', 'secret')
    admin = Admin(app, name='propel', template_mode='bootstrap3')
    admin.add_view(ModelView(Connections, Session))
    admin.add_view(ModelView(TaskGroups, Session))
    admin.add_view(ModelView(Tasks, Session))
    admin.add_view(TaskRunsView(TaskRuns, Session))
    admin.add_view(ModelView(Heartbeats, Session))
    admin.add_view(TweetsView(Tweets, Session))
    admin.add_view(TweetsDeckView(name='TweetsDeck'))

    # After the request response cycle is complete removing the scoped session.
    # Otherwise data added to the DB but external processes (like scheduler or worker )
    # are not viewable through the UI due to REPEATABLE_READ isolation in MySql.
    @app.teardown_appcontext
    def shutdown_session(response_or_exc):
        Session.remove()
        return response_or_exc

    return app
