import json

import service.prediction_service
from utils import log
from service import prediction_service
import pandas as pd
import logging.config

log.setup_logging()
logger = logging.getLogger('modelConsumerLog')

def handler(event, context):
    ad_client_id = event["ad_client_id"]
    if ad_client_id is None or ad_client_id == 0:
        return {
            'status' : 400,
            'message': 'ad_client_id is mandatory in JSON body'
        }
    df = pd.DataFrame(event["Input"])
    if df.empty:
        return {
            'status' : 200,
            'message': 'No data received for prediction'
        }
    try:
        logger.info(f'Received prediction request for client: {ad_client_id}')
        prediction_data = prediction_service.predict(df, ad_client_id)
        parsed_data = json.loads(prediction_data.to_json(orient='records'))
        return parsed_data
    except service.prediction_service.ClientException as e:
        logger.info(f'Unable to run prediction for client: {ad_client_id}, cause: {str(e)}')
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

