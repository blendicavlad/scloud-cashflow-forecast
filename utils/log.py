import logging
import os

import yaml
from logging.config import dictConfig


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
                config = yaml.safe_load(f.read())
                dictConfig(config)
            except Exception as e:
                print(e)
                print('Error in Logging Configuration. Using default configs')
                logging.basicConfig(level=default_level)
    else:
        logging.basicConfig(level=default_level)
        print('Failed to load configuration file. Using default configs')

