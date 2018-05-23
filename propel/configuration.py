import configparser
import os


config = configparser.ConfigParser()
cwd = os.path.dirname(os.path.realpath(__file__))
if os.path.exists(os.path.join(cwd, 'propel.cfg')):
    config_path = os.path.join(cwd, 'propel.cfg')
    config.read(config_path)
elif os.path.exists(os.path.join(cwd, 'default_config.cfg')):
    config_path = os.path.join(cwd, 'default_config.cfg')
    config.read(config_path)
else:
    raise Exception('Unable to find config at {}'.format(cwd))


def get(section, option=None):
    if option:
        return config.get(section, option)
    else:
        return config.items(section)
