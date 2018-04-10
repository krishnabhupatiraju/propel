import json
import logging

from requests_oauthlib import OAuth2Session

from propel import configuration
from propel.platforms.base import BasePlatform
from propel.models import Connections
from propel.utils.json_parser import perform_operations_on_json
from propel.utils.db import sessionize


class Twitter(BasePlatform):
    """
    Class that contains methods to capture and store tweets from Twitter
    """
    token = None
    timeline_url = configuration.get('urls', 'twitter_user_timeline')
    
    @staticmethod
    @sessionize
    def _get_token(session=None):
        twitter_conn = (session.
                        query(Connections).
                        filter_by(type='Twitter').
                        first())    
        token = json.loads(twitter_conn.token)
        return token

    @classmethod
    def get(cls, user_id, from_id=None, *args, **kwargs):
        """
        Capture tweets for a given Twitter user screen name
        
        :param user_id: screen name of Twitter user
        :type user_id: str
        :param from_id: id of the last downloaded tweet for this user. 
             If none the last 200 are downloaded 
        :type from_id: str
        """
        logging.info('Getting Twitter token')
        token = cls._get_token()
        twitter_session = OAuth2Session(token=token)
        continue_fetching = True
        request_params = {'screen_name': user_id,
                          'trim_user': 'true',
                          'exclude_replies': 'false',
                          'include_rts': 'true',
                          'count': 200
                          }
        if from_id:
            request_params['since_id'] = from_id
        while continue_fetching:
            twitter_response = twitter_session.get(cls.timeline_url,
                                                   params=request_params)
            twitter_response.raise_for_status()
            tweets = json.loads(twitter_response.text)
            if len(tweets) > 0:
                continue_fetching = True
                request_params['since_id'] = max([tweet['id'] for tweet in tweets])
            else:
                continue_fetching = False
            # Add logic to parse result and insert into DB
            for tweet in tweets:
                print tweet
    
    def schedule_args(self):
        """
        Returns the args using which the scheduler calls the 
        get method
        """
        pass

        
if __name__ == '__main__':
    print Twitter.get('b_krishna_varma')
