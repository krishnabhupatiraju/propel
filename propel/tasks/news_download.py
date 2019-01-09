import json
import feedparser

from propel.exceptions import PropelException
from propel.models import News
from propel.tasks.base_task import BaseTask
from propel.settings import logger


class NewsDownload(BaseTask):
    """
    Class that contains methods to capture and store RSS News Feeds
    """

    def execute(self, task):
        """
        Download news for a given set of filters e.g. sources, q, from, to, language etc

        :param task: An dict that contains details about the task to run
        :type task: dict
        """
        rss_url = json.loads(task['task_args'])['rss_url']
        rss_response = feedparser.parse(url_file_stream_or_string=rss_url)
        if rss_response.status != 200:
            raise PropelException('Non 200 response: {}'.format(rss_response.status))
        else:
            logger.info('Got {} news articles'.format(len(rss_response.entries)))
        News.load_into_db(rss_response)
