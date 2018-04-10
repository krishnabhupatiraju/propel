from datetime import datetime
from propel import configuration
from sqlalchemy import (create_engine, Table, Column, String, 
                        Integer, DateTime, Enum, Boolean, ForeignKey)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

db = configuration.get('core', 'db')
Base = declarative_base()
engine = create_engine(db, echo=False)
Session = sessionmaker(bind=engine)


class Connections(Base):
    __tablename__ = 'connections'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    type = Column(Enum('Twitter','Youtube'), nullable=False)
    key = Column(String(5000))
    secret = Column(String(5000))
    token = Column(String(5000))
    created_at = Column(DateTime, default=datetime.now)
    updated_on = Column(DateTime,
                        default=datetime.now,
                        onupdate=datetime.now)

    def __repr__(self):
        return ("<Connection(id={0}, name={1})>"
                .format(self.id,self.name))


# Association table to define Many to Many relationship
# between Flocks and birds
bird_flock = Table('bird_flock',
                   Base.metadata, 
                   Column('flock_id', Integer, ForeignKey('flocks.id')),
                   Column('bird_id', Integer, ForeignKey('birds.id'))
                   )


class Flocks(Base):
    __tablename__ = 'flocks'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    is_enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_on = Column(DateTime,
                        default=datetime.now,
                        onupdate=datetime.now)
    birds = relationship('Birds', 
                         backref = 'flocks',
                         secondary=bird_flock)

    def __repr__(self):
        return ("<Flock(id={0}, name={1}>"
                .format(self.id,self.name))


class Birds(Base):
    __tablename__ = 'birds'
    id = Column(Integer, primary_key=True)
    platform_id = Column(String(1000), nullable=False)
    platform_type = Column(Enum('Twitter'), nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_on = Column(DateTime,
                        default=datetime.now,
                        onupdate=datetime.now)
    tweets = relationship('Tweets', backref='bird')

    def __repr__(self):
        return ("<Bird(id={0}, platform_id={1}, platform_type={2})>"
                .format(self.id,self.platform_id, self.platform_type))


class Tweets(Base):
    __tablename__ = 'tweets'
    tweet_id = Column(String(255), primary_key=True)
    bird_id = Column(Integer, ForeignKey('birds.id'), nullable=False)
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
    created_at = Column(DateTime, default=datetime.now)
    updated_on = Column(DateTime,
                        default=datetime.now,
                        onupdate=datetime.now)

    def __repr__(self):
        return ("<Tweet(id={0}, text={1}, user_screen_name={2})>"
                .format(self.id,self.text, self.user_screen_name))


Base.metadata.create_all(engine)
