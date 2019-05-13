import json

from requests_oauthlib import OAuth2Session

from propel.models import Connections, Tweets, BaseTask
from propel.settings import logger
from propel.utils.db import provide_session


class TwitterExtract(BaseTask):
    """
    Class that contains methods to capture and store tweets from Twitter
    """
    token = None
    timeline_url = 'https://api.twitter.com/1.1/statuses/user_timeline.json'

    @staticmethod
    @provide_session
    def _get_token(session=None):
        twitter_conn = (
            session
            .query(Connections)
            .filter_by(type='Twitter')
            .first()
        )
        token = json.loads(twitter_conn.token)
        return token

    def execute(self, task):
        """
        Capture tweets for a given Twitter user screen name

        :param task: An dict that contains details about the task to run
        :type task: dict
        """
        logger.info('Getting Twitter Credentials')
        token = self._get_token()
        twitter_session = OAuth2Session(token=token)
        continue_fetching = True
        screen_name = task['task_args']
        # count: The API puts a limit of 200 tweets per requests
        request_params = {
            'screen_name': screen_name,
            'exclude_replies': 'false',
            'include_rts': 'true',
            'count': 200,
            'tweet_mode': 'extended'
        }
        logger.info("Fetching tweets for {}".format(screen_name))
        from_id = Tweets.latest_tweet_id_for_user(screen_name=screen_name)
        if from_id:
            request_params['since_id'] = from_id
        tweets = list()
        while continue_fetching:
            twitter_response = twitter_session.get(self.timeline_url, params=request_params)
            twitter_response.raise_for_status()
            paginated_tweets = json.loads(twitter_response.text)
            tweets.extend(paginated_tweets)
            if len(paginated_tweets) > 0:
                continue_fetching = True
                request_params['since_id'] = max(
                    request_params.get('since_id'),
                    max([tweet['id'] for tweet in paginated_tweets])
                )
            else:
                logger.info('Exhausted tweet timeline for {}'.format(screen_name))
                continue_fetching = False
        Tweets.load_into_db(tweets)
