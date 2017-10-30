import json
import logging
import time
import traceback

from propel.platforms.base import BasePlatform
from propel.celery import app
from propel.models import Session, Connections
from requests_oauthlib import OAuth2Session 



class Twitter(BasePlatform):
    """
    Class that contains methods to capture and store tweets from Twitter
    """
    token = None
    timeline_url = ('https://api.twitter.com/1.1/statuses/'
                             +' user_timeline.json')
    
    def __init__(self, try_limit=3, retry_delay=60):
        self.try_limit = try_limit
        self.retry_delay = retry_delay
        if not self.__class__.token:
            logging.info('Getting Twitter token')
            self.__class__.token = self.get_token()
    
    @classmethod
    def get_token(cls):
        session = Session()
        twitter_conn = (session.
                        query(Connections).
                        filter_by(type='Twitter').
                        first())    
        token = json.loads(twitter_conn.token)
        session.close()
        return token

    def get(self, user_id, from_id = None, *args, **kwargs):
        """
        Capture tweets for a given Twitter user screen name
        
        :param user_id: screen name of Twitter user
        :type user_id: str
        :param from_id: id of the last downloaded tweet for this user. 
             If none the last 200 are downloaded 
        :type from_id: str
        """
        token = self.__class__.token
        twitter_session = OAuth2Session(token=token)
        since_id = from_id
        for attempt_num in range(1, self.try_limit+1):
            try:
                log_msg = (('{{}} attempt {0} to download tweets for' +
                            ' user :{1} since tweet:{2}.')
                           .format(attempt_num, user_id, since_id))
                params = {'screen_name': user_id,
                          'trim_user':'true',
                          'exclude_replies':'false',
                          'include_rts': 'true',
                          'count':200
                    }
                if since_id:
                    params['since_id'] = since_id
                logging.info(log_msg.format('Starting'))
                tweets = twitter_session.get(self.timeline_url,
                                             params=params)
                tweets.raise_for_status()
                # Add logic to parse result and insert into DB
                # Add logic to extract the since_id and retry until [] 
                # is returned
            except Exception as e:
                logging.error(e, exc_info=True)
                logging.error(log_msg.format('Failed'))
                time.sleep(self.retry_delay)
                if attempt_num == self.try_limit:
                    logging.error('Attempt limit reached.' +
                                  ' Will not retry')
            else:
                logging.info(log_msg.format('Successful'))    
                break
    
    def schedule_args(self):
        """
        Returns the args using which the scheduler calls the 
        get method
        """
        pass
        
        

        
        
    
    