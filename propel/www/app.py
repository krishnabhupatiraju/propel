from collections import defaultdict
from flask import Flask, Markup
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from flask_admin import BaseView, expose

from propel import configuration

from propel.models import Connections, TaskGroups, Tasks, TaskRuns, \
    Heartbeats, Tweets, News, Article
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

class NewsView(ModelView):
    page_size = 50
    can_export = True
    column_filters = [
        'source',
        'published_at',
        'title'
    ]
    column_list = [
        'source',
        'published_at',
        'title',
        'summary',
        'url'
    ]
    can_create = False

    def create_anchor_link(view, context, model, url):
        return Markup(
            "<a href='{}' target='_blank'>Link</a>"
            .format(getattr(model, url))
        )

    column_formatters = dict(url=create_anchor_link)


class ArticlesDeckView(BaseView):
    @expose('/', methods=['GET', 'POST'])
    @provide_session
    def index(self, session=None):
        form = forms.ArticlesDeck()
        articles = defaultdict(list)
        authors = list()
        if form.validate_on_submit():
            authors = map(lambda x: x.strip(), form.authors.data.split(','))
            start_dt = form.start_dt.data
            end_dt = form.end_dt.data
            matching_tweets = (
                session
                .query(Tweets)
                .filter(Tweets.user_screen_name.in_(authors))
                .filter(Tweets.tweet_created_at >= start_dt)
                .filter(Tweets.tweet_created_at <= end_dt)
                .all()
            )
            for tweet in matching_tweets:
                article = Article(
                    author=tweet.user_screen_name,
                    article_type=tweet.tweet_type,
                    text=tweet.get_full_text(),
                    popularity=tweet.favorite_count,
                    url=tweet.get_tweet_url()
                )
                articles[tweet.user_screen_name].append(article)
            # Fetching News articles
            matching_news = (
                session
                .query(News)
                .filter(News.published_at >= start_dt)
                .filter(News.published_at <= end_dt)
                .all()
            )
            for news in matching_news:
                article = Article(
                    author=news.source,
                    article_type='News',
                    text=news.title,
                    popularity=news.published_at,
                    url=news.url
                )
                articles['News'].append(article)
            # Adding 'News' to the top of the list so it appears first in the page
            authors.insert(0, 'News')
            # Sorting articles based on popularity
            for author, article_list in articles.items():
                articles[author] = sorted(
                    article_list,
                    key=lambda x: x.popularity,
                    reverse=True
                )
        # Passing authors to display in same order as form input in table
        return self.render(
            'articles_deck.html',
            form=form,
            articles=articles,
            authors=authors
        )


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
    admin.add_view(NewsView(News, Session))
    admin.add_view(ArticlesDeckView(name='ArticlesDeck'))

    # After the request response cycle is complete removing the scoped session.
    # Otherwise data added to the DB but external processes (like scheduler or worker )
    # are not viewable through the UI due to REPEATABLE_READ isolation in MySql.
    @app.teardown_appcontext
    def shutdown_session(response_or_exc):
        Session.remove()
        return response_or_exc

    return app
