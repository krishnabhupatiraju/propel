import json

from requests_oauthlib import OAuth2Session

from propel import configuration
from propel.tasks.base_task import BaseTask
from propel.models import Connections, Tweets
from propel.settings import logger
from propel.utils.db import provide_session


class TwitterExtract(BaseTask):
    """
    Class that contains methods to capture and store tweets from Twitter
    """
    token = None
    timeline_url = configuration.get('urls', 'twitter_user_timeline')

    @staticmethod
    @provide_session
    def _get_token(session=None):
        twitter_conn = (session.
                        query(Connections).
                        filter_by(type='Twitter').
                        first())
        token = json.loads(twitter_conn.token)
        return token

    def execute(self, task):
        """
        Capture tweets for a given Twitter user screen name
        
        :param task: An dict that contains details about the task to run
        :type task: dict
        """
        logger.info('Getting Twitter token')
        token = self._get_token()
        twitter_session = OAuth2Session(token=token)
        continue_fetching = True
        request_params = {'screen_name': task['task_args'],
                          'trim_user': 'true',
                          'exclude_replies': 'false',
                          'include_rts': 'true',
                          'count': 200
                          }
        if from_id:
            request_params['since_id'] = from_id
        while continue_fetching:
            twitter_response = twitter_session.get(self.timeline_url,
                                                   params=request_params)
            twitter_response.raise_for_status()
            tweets = json.loads(twitter_response.text)
            if len(tweets) > 0:
                continue_fetching = True
                request_params['since_id'] = max([tweet['id'] for tweet in tweets])
            else:
                continue_fetching = False
            Tweets.insert_to_db(tweets)


if __name__ == '__main__':
    print TwitterExtract.get('b_krishna_varma')
