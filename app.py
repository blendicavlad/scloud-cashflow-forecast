import os

from flask import Flask
from utils import log
from service import py_data_cleaner
import logging.config

app = Flask(__name__)
log.setup_logging()
logger = logging.getLogger('appLog')


@app.route('/call_service', methods=['GET'])
def call_service():
    cleaner_service = py_data_cleaner.DataCleanerPy()
    state_map = cleaner_service.run()
    ret_map = {}
    for k, v in state_map.items():
        if v is True:
            ret_map[k] = "Success"
        else:
            ret_map[k] = "False"
    # server_utils.shutdown_server() Kill server after it finished executed
    return ret_map


if __name__ == '__main__':
    app.run(debug=bool(os.environ.get('DEBUG')), host='0.0.0.0')
