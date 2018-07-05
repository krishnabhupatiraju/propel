import json
import requests

from propel.models import Connections, News
from propel.tasks.base_task import BaseTask
from propel.settings import logger
from propel.utils.db import provide_session


class NewsDownload(BaseTask):
    """
    Class that contains methods to capture and store US news headlines.
    News is downloaded from NewsAPI.org
    """
    everything_url = 'https://newsapi.org/v2/everything'

    @staticmethod
    @provide_session
    def _get_token(session=None):
        news_conn = (
            session
            .query(Connections)
            .filter_by(type='NewsAPI')
            .first()
        )
        return news_conn.key

    def execute(self, task):
        """
        Download news for a given set of filters e.g. sources, q, from, to, language etc

        :param task: An dict that contains details about the task to run
        :type task: dict
        """
        # Due to json serialization datetime datatype is cast to unicode.
        # So no need to cast from_param and to to string
        newsapi_params = {
            'from': task['interval_start_ds'],
            'to': task['interval_end_ds']
        }
        newsapi_params.update(json.loads(task['task_args']))
        # Converting to string to ensure colon in dates is not URL encoded
        newsapi_params_str = "&".join(
            [
                "{}={}".format(k, v)
                for k, v in newsapi_params.iteritems()
            ]
        )
        logger.info('Getting news articles for {}'.format(newsapi_params))
        headers = {'Authorization': self._get_token()}
        newsapi_response = requests.get(
            self.everything_url,
            params=newsapi_params_str,
            headers=headers
        )
        newsapi_response.raise_for_status()
        articles = newsapi_response.json()['articles']
        logger.info('Got {} news articles'.format(len(articles)))
        if 'articles' in newsapi_response.json():
            News.load_into_db(articles)
