import json
import os
import traceback

import service.prediction_service
from utils import log
from service import prediction_service
import pandas as pd
import logging.config

log.setup_logging()
logger = logging.getLogger('modelConsumerLog')

def handler(event, context):
    try:
        ad_client_id = event["ad_client_id"]
    except KeyError:
        return {
            'status' : 400,
            'message': 'ad_client_id is mandatory in JSON body'
        }

    try:
        df = pd.DataFrame(event["Input"])
    except KeyError:
        return {
            'status' : 400,
            'message': 'ad_client_id is mandatory in JSON body'
        }

    if df.empty:
        return {
            'status' : 400,
            'message': 'Input is mandatory in JSON body'
        }
    try:
        logger.info(f'Received prediction request for client: {ad_client_id}')
        prediction_data = prediction_service.predict(df, ad_client_id)
        parsed_data = json.loads(prediction_data.to_json(orient='records'))
        return parsed_data
    except service.prediction_service.ClientException as e:
        logger.info(f'Unable to run prediction for client: {ad_client_id}, cause: {str(e)}')
        if bool(os.environ.get('DEBUG')):
            logger.error(traceback.format_exc())
        return {
            'status': 200,
            'message': str(e)
        }
    except Exception as e:
        logger.error(f'Error in running prediction for client {ad_client_id}, cause {str(e)}')
        return {
            'status': 500,
            'message': 'Server error'
        }

