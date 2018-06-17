import subprocess
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
            logger.info('Stopping Executor Worker')
        else:
            logger.info("Starting Executor Worker")
            executor = Executor()
            concurrency = cli_args.get('start')
            executor.start(concurrency)
    elif subparser_name == 'queue':
        if cli_args.get('stop'):
            logger.info("Stopping RabbitMQ")
        else:
            logger.info("Starting RabbitMQ")
            rabbitmq_cmd = ['rabbitmq-server']
            rabbitmq_process = subprocess.Popen(args=rabbitmq_cmd)
            logger.info(
                'Started RabbitMQ with PID: {}'
                .format(rabbitmq_process.pid)
            )
            rabbitmq_process.communicate()
    else:
        raise NotImplementedError()
