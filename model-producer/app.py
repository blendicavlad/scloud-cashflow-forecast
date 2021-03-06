import logging
import os

from service import pipeline_runner
from utils import log
import sentry_sdk

log.setup_logging()
logger = logging.getLogger('modelProducerLog')

sentry_sdk.init(
    os.environ.get('SENTRY_KEY'),
    traces_sample_rate=1.0
)


def call_service():
    logger.info('Service started')
    state_map = pipeline_runner.run()
    ret_map = {}
    for k, v in state_map.items():
        if v is True:
            ret_map[k] = "Success"
        else:
            ret_map[k] = "False"
    logger.info(str(state_map))


if __name__ == "__main__":
    call_service()
