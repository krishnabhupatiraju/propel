import json
import logging

from base import BasePlatform
from propel.celery_app import app
from propel.models import Session, Connections
from requests_oauthlib import OAuth2Session 



class Twitter(BasePlatform):
    """
    Class that contains methods to capture and store tweets from Twitter
    """
    
    def __init__(self):
        pass

    
    def capture_objects(self, user_id, from_id = 0, *args, **kwargs):
        """
        Capture tweets for a given Twitter user screen name
        
        :param user_id: screen name of Twitter user
        :type user_id: str
        :param from_id: id of the last downloaded tweet
        :type from_id: str
        """
        session = Session()
        twitter_conn = (session.
                        query(Connections).
                        filter_by(type='Twitter').
                        first())
        token = json.loads(twitter_conn.token)
        twitter_session = OAuth2Session(token=token)
        tweets = twitter_session.get('https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name=PrisonPlanet&trim_user=true&exclude_replies=false&include_rts=True&count=200')
        print json.loads(tweets.text)

        
        
    
    