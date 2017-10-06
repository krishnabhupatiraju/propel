from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
engine = create_engine('sqlite:///:memory:', echo=True)
Session = sessionmaker(bind=engine)

class Connection(Base):
    __tablename__ = 'connections'
    id = Column(Integer(), primary_key=True)
    conn_name = Column(String(255), nullable=False)
    conn_type = Column(String(255))
    key = Column(String(255))
    secret = Column(String(255))
    token = Column(String(255))
    created_at = Column(DateTime(), default = datetime.now)
    updated_on = Column(DateTime(),
                        default = datetime.now,
                        onupdate = datetime.now)

class Tweets(Base):
    tweet_id = Column(String(255))
    tweet_type = Column(String(255))
    created_at = Column(DateTime())
    text = Column(String(255))
    media_url = Column(String(255)) # extended_entities.media.media_url for images
    # For video uploaded to twitter the media url contains a pic.
    # The u'type' = u'video' and extended_entities.media.video_info.variants contains the video link
    media_type = Column(String(255)) #extended_entities.media.video_info.variants.u'content_type' = 'video/mp4'[0].'url'
    media_video_url = Column(String(255))
    expanded_url = Column(String(255)) # entities.urls.expanded_url for video, html links
    favorite_count = Column(Integer())
    retweet_count = Column(Integer())
    user_id = Column(String(255))
    user_screen_name = Column(String(255))

    in_reply_to_tweet_id = Column(String(255)) # populated from u'in_reply_to_status_id_str
    in_reply_to_user_id = Column(String(255))
    in_reply_to_user_screen_name = Column(String(255))

    quoted_tweet_id = Column(String(255))
    quoted_created_at = Column(DateTime()) #u'quoted_status'.u'created_at'
    quoted_text = Column(String(255))
    quoted_media_url = Column(String(255)) # quoted_status.entities.media.media_url
    quoted_media_type = Column(String(255)) #quoted_status.extended_entities.media.video_info.variants.u'content_type' = 'video/mp4'[0].'url'
    quoted_media_video_url = Column(String(255))
    quoted_expanded_url = Column(String(255))
    quoted_favorite_count = Column(Integer())
    quoted_retweet_count = Column(Integer())
    quoted_user_id = Column(String(255))
    quoted_user_screen_name = Column(String(255))

    retweet_tweet_id = Column(String(255))
    retweet_created_at = Column(DateTime()) #u'retweeted_status'.u'created_at'
    retweet_text = Column(String(255))
    retweet_media_url = Column(String(255)) # retweeted_status.entities.media.media_url
    retweet_media_type = Column(String(255)) #retweeted_status.extended_entities.media.video_info.variants.u'content_type' = 'video/mp4'[0].'url'
    retweet_media_video_url = Column(String(255))
    retweet_expanded_url = Column(String(255))
    retweet_favorite_count = Column(Integer())
    retweet_retweet_count = Column(Integer())
    retweet_user_id = Column(String(255))
    retweet_user_screen_name = Column(String(255))



Base.metadata.create_all(engine)
