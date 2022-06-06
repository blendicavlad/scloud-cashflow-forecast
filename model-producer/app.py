import logging
import time

from service import pipeline_runner
from utils import log
from service import file_service

log.setup_logging()
logger = logging.getLogger('modelProducerLog')


def call_service():
    logger.info('Service started')
    runner = pipeline_runner.PipelineRunner()
    state_map = runner.run()
    ret_map = {}
    for k, v in state_map.items():
        if v is True:
            ret_map[k] = "Success"
        else:
            ret_map[k] = "False"
    logger.info(str(state_map))


if __name__ == "__main__":
    call_service()
