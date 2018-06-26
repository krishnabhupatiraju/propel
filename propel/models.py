from datetime import datetime
from sqlalchemy import (Table, Column, String, Integer, BigInteger, JSON,
                        DateTime, Enum, Boolean, ForeignKey)
from sqlalchemy.dialects.mysql import BIGINT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, relationship
from sqlalchemy.sql import func

from propel.settings import Engine
from propel.utils.db import provide_session
from propel.utils.general import extract_from_json

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


# Association table to define Many to Many relationship
# between Task Groups and Tasks
task_group_members = Table('task_group_members',
                           Base.metadata,
                           Column('task_group_id', Integer, ForeignKey('task_groups.id')),
                           Column('task_id', Integer, ForeignKey('tasks.id'))
                           )


class TaskGroups(Base):
    __tablename__ = 'task_groups'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    is_enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    tasks = relationship('Tasks', backref='task_groups', secondary=task_group_members)

    def __repr__(self):
        return "<Task Group(id={0}, name={1})>".format(self.id, self.name)


class Tasks(Base):
    __tablename__ = 'tasks'
    id = Column(Integer, primary_key=True)
    task_name = Column(String(1000), nullable=False)
    task_type = Column(Enum('TwitterExtract'), nullable=False)
    task_args = Column(String(1000), nullable=False)
    run_frequency_seconds = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return (
            "<Task(id={0}, task_type={2}, task_args={1})>"
            .format(self.id, self.task_args, self.task_type)
        )

    def as_dict(self):
        return {
            column_name: getattr(self, column_name)
            for column_name in self.__mapper__.c.keys()
        }


class TaskRuns(Base):
    __tablename__ = 'task_runs'
    id = Column(Integer, primary_key=True)
    task_id = Column(Integer, ForeignKey('tasks.id'), nullable=False)
    state = Column(String(255), nullable=False)
    run_ds = Column(DateTime, nullable=False)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    tasks = relationship('Tasks', backref=backref('task_runs', lazy='dynamic'))

    def __repr__(self):
        return (
            "<TaskRun(id={0}, task_id={1}, run_ds={2}, state={3})>"
            .format(self.id, self.task_id, self.run_ds, self.state)
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
            .format(self.id, self.text, self.user_screen_name)
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
        tweets_date_format = '%a %b %d %H:%M:%S +0000 %Y'
        for tweet_json in tweets_json:
            tweet_dict = dict()
            tweet_dict['tweet_id'] = tweet_json['id']
            tweet_dict['tweet_created_at'] = datetime.strptime(
                tweet_json.get('created_at'),
                tweets_date_format
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
                    tweets_date_format
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
                    tweets_date_format
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


Base.metadata.create_all(Engine)
