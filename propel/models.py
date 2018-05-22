from datetime import datetime
from sqlalchemy import (Table, Column, String, Integer,
                        DateTime, Enum, Boolean, ForeignKey)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, relationship

from propel.settings import Engine
from propel.utils.db import provide_session

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
            .format(self.id,self.name)
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
        return "<Task Group(id={0}, name={1})>".format(self.id,self.name)


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
    tweet_id = Column(String(255), primary_key=True)
    tweet_type = Column(String(255))
    # Derived from parent level attributes of JSON returned by 
    # statuses/user_timeline
    tweet_created_at = Column(DateTime)
    text = Column(String(255))
    # Media uploaded to twitter
    # Derived from extended_entities.media[].media_url
    # In case of images it contains the url to the image
    # In case of video contains a preview image of the video
    media_urls = Column(String(1000))
    # Derived from extended_entities.media[].type.
    # Values are photo or video
    media_types = Column(String(1000))
    # Derived from extended_entities.media[].video_info.variants.
    # content_type = 'video/mp4'[0].url
    media_video_urls = Column(String(1000))
    # Derived from entities.urls[].expanded_url. Contains links that 
    # were part of the tweet
    expanded_urls = Column(String(1000))
    favorite_count = Column(Integer)
    retweet_count = Column(Integer)
    user_id = Column(String(255))
    user_screen_name = Column(String(1000))
    # Populated only for tweet_type = 'REPLY'
    # Derived from u'in_reply_to_status_id_str
    in_reply_to_tweet_id = Column(String(255)) 
    in_reply_to_user_id = Column(String(255))
    in_reply_to_user_screen_name = Column(String(1000))
    # Populated only for tweet_type = 'QUOTED RETWEET'
    # Fields derived from "quoted_status" node within the json
    quoted_tweet_id = Column(String(255))
    quoted_created_at = Column(DateTime)
    quoted_text = Column(String(255))
    quoted_media_urls = Column(String(1000))
    quoted_media_types = Column(String(1000))
    quoted_media_video_urls = Column(String(1000))
    quoted_expanded_urls = Column(String(1000))
    quoted_favorite_count = Column(Integer)
    quoted_retweet_count = Column(Integer)
    quoted_user_id = Column(String(255))
    quoted_user_screen_name = Column(String(1000))
    # Populated only for tweet_type = 'RETWEET'
    # Fields derived from "retweeted_status" node within the json
    retweet_tweet_id = Column(String(255))
    retweet_created_at = Column(DateTime)
    retweet_text = Column(String(255))
    retweet_media_urls = Column(String(1000))
    retweet_media_types = Column(String(1000))
    retweet_media_video_urls = Column(String(1000))
    retweet_expanded_urls = Column(String(1000))
    retweet_favorite_count = Column(Integer)
    retweet_retweet_count = Column(Integer)
    retweet_user_id = Column(String(255))
    retweet_user_screen_name = Column(String(1000))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return (
            "<Tweet(id={0}, text={1}, user_screen_name={2})>"
            .format(self.id,self.text, self.user_screen_name)
        )

    @classmethod
    @provide_session
    def insert_to_db(cls, tweets_json, session=None):
        tweets = list()
        for tweet_json in tweets_json:
            tweets.append(cls(tweet_id=tweet_json['id']))
        session.bulk_save_objects(tweets)


Base.metadata.create_all(Engine)
