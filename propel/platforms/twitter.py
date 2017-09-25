import logging

from base import BasePlatform
from oauthlib.oauth2 import BackendApplicationClient
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
        
        
    
    