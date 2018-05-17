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
    elif subparser_name == 'celery':
        print "Starting Celery"
    elif subparser_name == 'queue':
        print "Starting Queue"
    else:
        raise NotImplementedError()
