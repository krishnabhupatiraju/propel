from propel.executors import Executor
from propel.scheduler import Scheduler
from propel.settings import logger
from propel.www.app import create_app


def cli_factory(cli_args):
    """
    Process CLI arguments and call required propel functions

    :param cli_args: Parsed arguments by argparse
    :type cli_args: argparse.Namespace
    """
    subparser_name = cli_args.get('subparser_name')
    if subparser_name == 'webserver':
        if cli_args.get('stop'):
            logger.info('Stopping Webserver')
        else:
            port = cli_args.get('port')
            logger.info('Starting Webserver at port {}'.format(port))
            app = create_app()
            app.run(port=port, debug=True)
    elif subparser_name == 'scheduler':
        if cli_args.get('stop'):
            logger.info('Stopping Scheduler')
        else:
            logger.info("Starting Scheduler")
            scheduler = Scheduler()
            scheduler.run()
    elif subparser_name == 'executor':
        if cli_args.get('stop'):
            logger.info('Stopping Executor Worker'.format(cli_args.get('stop')))
        else:
            logger.info("Starting Executor Worker")
            executor = Executor()
            concurrency = cli_args.get('start')
            executor.start(concurrency)
    elif subparser_name == 'queue':
        logger.info("Starting RabbitMQ")
    else:
        raise NotImplementedError()
