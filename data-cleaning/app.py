
from utils import log
from service import data_cleaning_service
import logging

log.setup_logging()
logger = logging.getLogger('appLog')


def call_service():
    cleaner_service = data_cleaning_service.DataCleaningService()
    state_map = cleaner_service.run()
    ret_map = {}
    for k, v in state_map.items():
        if v is True:
            ret_map[k] = "Success"
        else:
            ret_map[k] = "False"
    return ret_map


if __name__ == "__main__":
    call_service()
