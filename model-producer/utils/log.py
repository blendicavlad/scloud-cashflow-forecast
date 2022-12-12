import logging
import os
import re
import watchtower

import yaml
from logging.config import dictConfig

env_var_path_matcher = re.compile(r'.*\${([^}^{]+)}.*')


def path_constructor(loader, node):
    return os.path.expandvars(node.value)


class EnvVarLoader(yaml.SafeLoader):
    pass


EnvVarLoader.add_implicit_resolver('!path', env_var_path_matcher, None)
EnvVarLoader.add_constructor('!path', path_constructor)


class InfoFilter(logging.Filter):
    def filter(self, log_record):
        return log_record.levelno == logging.INFO


def setup_logging(default_path='logging.yaml', default_level=logging.INFO, env_key='LOG_CFG'):
    """
    :param default_path: YAML configuration file path
    :param default_level: logging level
    :param env_key: env key for log file
    """
    path = default_path
    env_path = None
    if env_key is None:
        env_path = None
    else:
        env_path = os.getenv(env_key, None)
        if env_path:
            path = env_path
    if os.path.exists(path):
        with open(path, 'rt') as f:
            try:
                config = yaml.load(f.read(), Loader=EnvVarLoader)
                dictConfig(config)
            except Exception as e:
                print(e)
                print('Error in Logging Configuration. Using default configs')
                logging.basicConfig(level=default_level)
    else:
        logging.basicConfig(level=default_level)
        print('Failed to load configuration file. Using default configs')
    logger = logging.getLogger('modelProducerLog')
    logger.addHandler(
        watchtower.CloudWatchLogHandler(log_group_name=os.environ.get('LOG_GROUP'),
                                        log_stream_name=os.environ.get('LOG_STREAM')))
    set_boto3_logging()
    logging.getLogger('matplotlib').setLevel(logging.INFO)
    logging.getLogger('matplotlib.font_manager').disabled = True
    logging.getLogger('matplotlib').disabled = True

def set_boto3_logging():
    logging.getLogger('boto').setLevel(logging.INFO)
    logging.getLogger('boto3').setLevel(logging.INFO)
    logging.getLogger('botocore').setLevel(logging.INFO)
    logging.getLogger('s3transfer').setLevel(logging.INFO)
    logging.getLogger('urllib3').setLevel(logging.INFO)
