import hashlib
import importlib
import inspect
import json
import os
from datetime import datetime
import networkx as nx
from sqlalchemy import (Table, Column, String, Integer, BigInteger, JSON,
                        DateTime, Enum, Boolean, ForeignKey)
from sqlalchemy.dialects.mysql import BIGINT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, relationship
from sqlalchemy.sql import func
from time import mktime, struct_time

from propel import configuration
from propel.exceptions import PropelException
from propel.settings import logger
from propel.utils.db import provide_session
from propel.utils.general import extract_from_json, add_path

Base = declarative_base()


class Connections(Base):
    __tablename__ = 'connections'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    type = Column(Enum('Twitter', 'Youtube'), nullable=False)
    key = Column(String(5000))
    secret = Column(String(5000))
    token = Column(String(5000))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return (
            "<Connection(id={0}, name={1})>"
            .format(self.id, self.name)
        )

class DagBag(object):
    """
    A collection of dags that are parsed from the dags_location
    """
    def __init__(self):
        self.dags_location = configuration.get('core', 'dags_location')
        self.dags = list()

    def parse_dags(self):
        with add_path(self.dags_location):
            for dirpath, _, filenames in os.walk(self.dags_location, followlinks=True):
                for filename in filenames:
                    relative_file_path = os.path.join(
                        os.path.relpath(dirpath, self.dags_location),
                        filename
                    )
                    full_file_path = os.path.join(dirpath, filename)
                    # Expect DAG files to be python files with 'DAG' word in the file
                    if not filename.endswith('.py'):
                        continue
                    with open(full_file_path) as f:
                        if 'DAG' not in f.read():
                            continue
                    try:
                        module_name = relative_file_path.replace('/', '.').replace('.py', '')
                        imported_module = importlib.import_module(module_name)
                        for _, item in inspect.getmembers(imported_module):
                            if isinstance(item, DAG):
                                self.dags.append(item)
                                item.dag_location = full_file_path
                    except Exception as e:
                        logger.warn("Could not import module {}".format(relative_file_path))
                        logger.warn(e)


class DAG(object):
    """
    DAG Object to which tasks are added
    """
    def __init__(
            self,
            dag_id,
            description,
            is_scheduled,
            start_date,
            interval,
    ):
        self.dag_id = dag_id
        self.description = description
        self.is_scheduled = is_scheduled
        self.start_date = start_date
        self.interval = interval
        self.dag = nx.DiGraph()
        self._dag_location = None

    def __repr__(self):
        return (
            "<DAG(dag_id={0}, description={1})>"
            .format(self.dag_id, self.description)
        )

    @property
    def dag_location(self):
        return self._dag_location

    @dag_location.setter
    def dag_location(self, dag_location):
        self._dag_location = dag_location

    def add_task(self, task):
        if not isinstance(task, BaseTask):
            raise PropelException("{} is not a Task. Only tasks can added to a DAG".format(task))
        self.dag.add_node(task)
        task.dag = self

    def add_upstream(self, task, upstream_task):
        self.dag.add_edge(upstream_task, task)

    def add_downstream(self, task, downstream_task):
        self.dag.add_edge(task, downstream_task)

    def is_valid(self):
        return nx.is_directed_acyclic_graph(self.dag)

    def get_tasks(self):
        return self.dag.nodes

    def get_task_ids(self):
        task_ids = list()
        for task in self.get_tasks():
            task_ids.append(task.task_id)
        return task_ids

    def is_task_id_in_dag(self, task_id):
        for task in self.get_tasks():
            if task.task_id == task_id:
                return True, task
        return False, None

    def get_upstream_tasks(self, task_id):
        is_task_id_in_dag, task = self.is_task_id_in_dag(task_id)
        if is_task_id_in_dag:
            return nx.ancestors(self.dag, task)
        return None

    def get_downstream_tasks(self, task_id):
        is_task_id_in_dag, task = self.is_task_id_in_dag(task_id)
        if is_task_id_in_dag:
            return nx.descendants(self.dag, task)
        return None


class DagRuns(Base):
    __tablename__ = 'dag_runs'
    id = Column(Integer, primary_key=True)
    dag_id = Column(String(255), nullable=False)
    run_ds = Column(DateTime, nullable=False)
    state = Column(String(255), nullable=False)

    def __repr__(self):
        return (
            "<DagRun(id={0}, dag_id={1}, run_ds={2}, state={3})>"
            .format(self.id, self.dag_id, self.run_ds, self.state)
        )


class TaskRuns(Base):
    __tablename__ = 'task_runs'
    id = Column(Integer, primary_key=True)
    dag_id = Column(String(255), nullable=False)
    task_id = Column(String(255), nullable=False)
    state = Column(String(255), nullable=False)
    run_ds = Column(DateTime, nullable=False)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return (
            "<TaskRun(id={0}, dag_id={1}, task_id={1}, run_ds={2}, state={3})>"
            .format(self.id, self.dag_id, self.task_id, self.run_ds, self.state)
        )


class TaskRunDependencies(Base):
    __tablename__ = 'task_run_dependencies'
    id = Column(Integer, primary_key=True)
    task_run_id = Column(Integer, ForeignKey('task_runs.id'), nullable=False)
    upstream_task_id = Column(Integer, ForeignKey('task_runs.id'), nullable=False)
    task = relationship('TaskRuns', foreign_keys=[task_run_id], backref=backref('upstream_dependencies'))
    upstream_task = relationship('TaskRuns', foreign_keys=[upstream_task_id], backref=backref('downstream_dependencies'))

    def __repr__(self):
        return (
            "<TaskRunDependency(task_run_id={0}, upstream_task_run_id={1})>"
            .format(self.id, self.task_run_id, self.upstream_task_id)
        )


class Heartbeats(Base):
    __tablename__ = 'heartbeats'
    id = Column(Integer, primary_key=True)
    task_run_id = Column(Integer, ForeignKey('task_runs.id'), nullable=True)
    task_type = Column(String(255), nullable=False)
    heartbeat_start_time = Column(DateTime, default=datetime.utcnow)
    last_heartbeat_time = Column(DateTime, default=datetime.utcnow)
    task_runs = relationship('TaskRuns', backref=backref('Heartbeats', lazy='dynamic'))

    def __repr__(self):
        return (
            "<Heartbeat(id={0}, task_run_id={1}, task_type={2}, last_heartbeat_time={3})>"
            .format(self.id, self.task_run_id, self.task_type, self.last_heartbeat_time)
        )


class Tweets(Base):
    __tablename__ = 'tweets'
    tweet_id = Column(BIGINT(unsigned=True), primary_key=True)
    tweet_type = Column(String(255))
    # Derived from parent level attributes of JSON returned by statuses/user_timeline
    tweet_created_at = Column(DateTime)
    text = Column(String(1000))
    # Media uploaded to twitter
    # Derived from extended_entities.media[].media_url
    # In case of images it contains the url to the image
    # In case of video contains a preview image of the video
    media_urls = Column(JSON)
    # Derived from extended_entities.media[].type.
    # Values are photo or video
    media_types = Column(JSON)
    # Derived from extended_entities.media[].video_info.variants[].
    # content_type = 'video/mp4'[0].url
    media_video_urls = Column(JSON)
    # Derived from entities.urls[].expanded_url. Contains links that
    # were part of the tweet
    expanded_urls = Column(JSON)
    favorite_count = Column(Integer)
    retweet_count = Column(Integer)
    user_id = Column(BigInteger)
    user_screen_name = Column(String(100))
    # Populated only for tweet_type = 'REPLY'
    # Derived from 'in_reply_to_status_id'
    in_reply_to_tweet_id = Column(BIGINT(unsigned=True))
    in_reply_to_user_id = Column(BigInteger)
    in_reply_to_screen_name = Column(String(100))
    # Populated only for tweet_type = 'QUOTED RETWEET'
    # Fields derived from "quoted_status" node within the json
    quoted_tweet_id = Column(BIGINT(unsigned=True))
    quoted_created_at = Column(DateTime)
    quoted_text = Column(String(1000))
    quoted_media_urls = Column(JSON)
    quoted_media_types = Column(JSON)
    quoted_media_video_urls = Column(JSON)
    quoted_expanded_urls = Column(JSON)
    quoted_favorite_count = Column(Integer)
    quoted_retweet_count = Column(Integer)
    quoted_user_id = Column(BigInteger)
    quoted_user_screen_name = Column(String(100))
    # Populated only for tweet_type = 'RETWEET'
    # Fields derived from "retweeted_status" node within the json
    retweet_tweet_id = Column(BIGINT(unsigned=True))
    retweet_created_at = Column(DateTime)
    retweet_text = Column(String(1000))
    retweet_media_urls = Column(JSON)
    retweet_media_types = Column(JSON)
    retweet_media_video_urls = Column(JSON)
    retweet_expanded_urls = Column(JSON)
    retweet_favorite_count = Column(Integer)
    retweet_retweet_count = Column(Integer)
    retweet_user_id = Column(BigInteger)
    retweet_user_screen_name = Column(String(100))
    raw_tweet = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return (
            "<Tweet(id={0}, text={1}, user_screen_name={2})>"
            .format(self.tweet_id, self.text, self.user_screen_name)
        )

    @classmethod
    @provide_session
    def latest_tweet_id_for_user(cls, screen_name, session=None):
        """
        Get the latest tweet id stored in the database for a given user_id

        :param screen_name: user_id
        :type screen_name: String
        """
        return (
            session
            .query(func.max(cls.tweet_id))
            .filter(cls.user_screen_name == screen_name)
            .scalar()
        )

    @classmethod
    @provide_session
    def load_into_db(cls, tweets_json, session=None):
        tweets = list()
        date_format = '%a %b %d %H:%M:%S +0000 %Y'
        for tweet_json in tweets_json:
            tweet_dict = dict()
            tweet_dict['tweet_id'] = tweet_json['id']
            tweet_dict['tweet_created_at'] = datetime.strptime(
                tweet_json.get('created_at'),
                date_format
            )
            # Based on the tweet_mode param passed to user_timeline data is
            # returned in text of full_text attribute
            tweet_dict['text'] = tweet_json.get('text') or tweet_json.get('full_text')
            tweet_dict['media_urls'] = extract_from_json(
                tweet_json,
                (
                    'extended_entities'
                    '.media'
                    '.[*]'
                    '.media_url'
                )
            )
            tweet_dict['media_types'] = extract_from_json(
                tweet_json,
                (
                    'extended_entities'
                    '.media'
                    '.[*]'
                    '.type'
                )
            )
            tweet_dict['media_video_urls'] = extract_from_json(
                tweet_json,
                (
                    'extended_entities'
                    '.media'
                    '.[*]'
                    '.video_info'
                    '.variants'
                    '.[*]'
                    '.{content_type == "video/mp4"}'
                    '.[0]'
                    '.url'
                )
            )
            tweet_dict['expanded_urls'] = extract_from_json(
                tweet_json,
                (
                    'entities'
                    '.urls'
                    '.[*]'
                    '.expanded_url'
                )
            )
            tweet_dict['favorite_count'] = tweet_json.get('favorite_count')
            tweet_dict['retweet_count'] = tweet_json.get('retweet_count')
            tweet_dict['user_id'] = extract_from_json(
                tweet_json,
                (
                    'user'
                    '.id'
                )
            )
            tweet_dict['user_screen_name'] = extract_from_json(
                tweet_json,
                (
                    'user'
                    '.screen_name'
                )
            )
            tweet_dict['in_reply_to_tweet_id'] = tweet_json.get('in_reply_to_status_id')
            tweet_dict['in_reply_to_user_id'] = tweet_json.get('in_reply_to_user_id')
            tweet_dict['in_reply_to_screen_name'] = tweet_json.get('in_reply_to_screen_name')
            tweet_dict['quoted_tweet_id'] = extract_from_json(
                tweet_json,
                (
                    'quoted_status'
                    '.id'
                )
            )

            quoted_created_at = extract_from_json(
                tweet_json,
                (
                    'quoted_status'
                    '.created_at'
                )
            )
            if quoted_created_at:
                tweet_dict['quoted_created_at'] = datetime.strptime(
                    quoted_created_at,
                    date_format
                )
            else:
                tweet_dict['quoted_created_at'] = None
            # Based on the tweet_mode param passed to user_timeline data is
            # returned in text of full_text attribute
            tweet_dict['quoted_text'] = (
                extract_from_json(
                    tweet_json,
                    (
                        'quoted_status'
                        '.text'
                    )
                ) or
                extract_from_json(
                    tweet_json,
                    (
                        'quoted_status'
                        '.full_text'
                    )
                )
            )
            tweet_dict['quoted_media_urls'] = extract_from_json(
                tweet_json,
                (
                    'quoted_status'
                    '.extended_entities'
                    '.media'
                    '.[*]'
                    '.media_url'
                )
            )
            tweet_dict['quoted_media_types'] = extract_from_json(
                tweet_json,
                (
                    'quoted_status'
                    '.extended_entities'
                    '.media'
                    '.[*]'
                    '.type'
                )
            )
            tweet_dict['quoted_media_video_urls'] = extract_from_json(
                tweet_json,
                (
                    'quoted_status'
                    '.extended_entities'
                    '.media'
                    '.[*]'
                    '.video_info'
                    '.variants'
                    '.[*]'
                    '.{content_type == "video/mp4"}'
                    '.[0]'
                    '.url'
                )
            )
            tweet_dict['quoted_expanded_urls'] = extract_from_json(
                tweet_json,
                (
                    'quoted_status'
                    '.entities'
                    '.urls'
                    '.[*]'
                    '.expanded_url'
                )
            )
            tweet_dict['quoted_favorite_count'] = extract_from_json(
                tweet_json,
                (
                    'quoted_status'
                    '.favorite_count'
                )
            )
            tweet_dict['quoted_retweet_count'] = extract_from_json(
                tweet_json,
                (
                    'quoted_status'
                    '.retweet_count'
                )
            )
            tweet_dict['quoted_user_id'] = extract_from_json(
                tweet_json,
                (
                    'quoted_status'
                    '.user'
                    '.id'
                )
            )
            tweet_dict['quoted_user_screen_name'] = extract_from_json(
                tweet_json,
                (
                    'quoted_status'
                    '.user'
                    '.screen_name'
                )
            )
            tweet_dict['retweet_tweet_id'] = extract_from_json(
                tweet_json,
                (
                    'retweeted_status'
                    '.id'
                )
            )

            retweet_created_at = extract_from_json(
                tweet_json,
                (
                    'retweeted_status'
                    '.created_at'
                )
            )
            if retweet_created_at:
                tweet_dict['retweet_created_at'] = datetime.strptime(
                    retweet_created_at,
                    date_format
                )
            else:
                tweet_dict['retweet_created_at'] = None
            # Based on the tweet_mode param passed to user_timeline data is
            # returned in text of full_text attribute
            tweet_dict['retweet_text'] = (
                    extract_from_json(
                        tweet_json,
                        (
                            'retweeted_status'
                            '.text'
                        )
                    ) or
                    extract_from_json(
                        tweet_json,
                        (
                            'retweeted_status'
                            '.full_text'
                        )
                    )
            )
            tweet_dict['retweet_media_urls'] = extract_from_json(
                tweet_json,
                (
                    'retweeted_status'
                    '.extended_entities'
                    '.media'
                    '.[*]'
                    '.media_url'
                )
            )
            tweet_dict['retweet_media_types'] = extract_from_json(
                tweet_json,
                (
                    'retweeted_status'
                    '.extended_entities'
                    '.media'
                    '.[*]'
                    '.type'
                )
            )
            tweet_dict['retweet_media_video_urls'] = extract_from_json(
                tweet_json,
                (
                    'retweeted_status'
                    '.extended_entities'
                    '.media'
                    '.[*]'
                    '.video_info'
                    '.variants'
                    '.[*]'
                    '.{content_type == "video/mp4"}'
                    '.[0]'
                    '.url'
                )
            )
            tweet_dict['retweet_expanded_urls'] = extract_from_json(
                tweet_json,
                (
                    'retweeted_status'
                    '.entities'
                    '.urls'
                    '.[*]'
                    '.expanded_url'
                )
            )
            tweet_dict['retweet_favorite_count'] = extract_from_json(
                tweet_json,
                (
                    'retweeted_status'
                    '.favorite_count'
                )
            )
            tweet_dict['retweet_retweet_count'] = extract_from_json(
                tweet_json,
                (
                    'retweeted_status'
                    '.retweet_count'
                )
            )
            tweet_dict['retweet_user_id'] = extract_from_json(
                tweet_json,
                (
                    'retweeted_status'
                    '.user'
                    '.id'
                )
            )
            tweet_dict['retweet_user_screen_name'] = extract_from_json(
                tweet_json,
                (
                    'retweeted_status'
                    '.user'
                    '.screen_name'
                )
            )
            tweet_dict['raw_tweet'] = tweet_json
            # Logic to derive type of tweet
            if tweet_dict['in_reply_to_tweet_id']:
                tweet_dict['tweet_type'] = 'Reply Tweet'
            elif tweet_dict['quoted_tweet_id']:
                tweet_dict['tweet_type'] = 'Quoted Tweet'
            elif tweet_dict['retweet_tweet_id']:
                tweet_dict['tweet_type'] = 'Retweet'
            else:
                tweet_dict['tweet_type'] = 'Tweet'
            # Collecting tweet object to bulk update at the end
            tweets.append(cls(**tweet_dict))
        session.bulk_save_objects(tweets)

    def get_full_text(self):
        """
        Return the full text of the tweet. The full text is available
        in one of the 3 text fields. This method uses the tweet_type
        field to determine and return that text field.
        """
        if self.tweet_type == 'Quoted Tweet':
            return self.quoted_text
        elif self.tweet_type == 'Retweet':
            return self.retweet_text
        else:
            return self.text

    def get_tweet_url(self):
        return 'https://twitter.com/i/web/status/{}'.format(self.tweet_id)


class News(Base):
    __tablename__ = 'news'
    news_id = Column(String(64), primary_key=True)
    source = Column(String(100))
    title = Column(String(2000))
    summary = Column(String(2000))
    url = Column(String(1000))
    published_at = Column(DateTime)
    raw_news = Column(JSON)

    def __repr__(self):
        return (
            "<News(id={0}, title={1}, source={2})>"
            .format(self.news_id, self.title, self.source)
        )

    @classmethod
    @provide_session
    def load_into_db(cls, news_feed_dict, session=None):
        try:
            source = news_feed_dict['feed']['link']
        except KeyError as ke:
            logger.exception(ke)
            raise PropelException('Unable to parse News feed Dict')
        news_entries = news_feed_dict.get('entries')
        for news_entry in news_entries:
            news_dict = dict()
            news_dict['news_id'] = hashlib.sha256(news_entry.get('link')).hexdigest()
            news_dict['source'] = source
            news_dict['title'] = news_entry.get('title')
            news_dict['summary'] = news_entry.get('summary')
            news_dict['url'] = news_entry.get('link')
            if news_entry.get('published_parsed'):
                news_dict['published_at'] = datetime.fromtimestamp(
                    mktime(news_entry.get('published_parsed'))
                )
            else:
                news_dict['published_at'] = datetime.utcnow()
            news_dict['raw_news'] = json.dumps(news_entry, default=cls._python_object_converter)
            # Merging newly created News
            session.merge(cls(**news_dict))

    @staticmethod
    def _python_object_converter(o):
        """
        Helper function called by json dumps when the python object cannot be converted to json by
        json parser
        """
        if isinstance(o, datetime) or isinstance(o, struct_time):
            return o.__str__()


class Article(object):

    def __init__(
            self,
            author,
            article_type,
            text,
            popularity,
            url
    ):
        self.author = author
        self.article_type = article_type
        self.text = text
        self.popularity = popularity
        self.url = url


class BaseTask(object):
    """
    Abstract BaseTask class that concrete subclasses will
    implement
    """

    def __init__(
            self,
            task_id,
            params=None,
            dag=None
    ):
        self.task_id = task_id
        if params:
            self.params = params
        else:
            self.params = dict()
        if dag:
            dag.add_task(self)
            self.dag = dag

    def execute(self, task):
        """
        Execute task

        :param task: An dict that contains details about the task to run
        :type task: dict
        """
        raise NotImplementedError()
