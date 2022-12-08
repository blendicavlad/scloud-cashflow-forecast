import logging
import traceback

from pandas import DataFrame
from datasource.db_api import DB_Interface
from datasource.db_props import DATA_CLEANING_SCHEMA
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, wait, ALL_COMPLETED
from .classification_pipeline import ClassificationPipeline
from .regression_pipeline import RegressionPipeline
import pandas.io.sql as pandas_sql
from sentry_sdk import start_transaction

logger = logging.getLogger('modelProducerLog')


def run():
    ad_client_ids = [1000298]
    with DB_Interface() as db_api:
        # rows = db_api.fetch_many(f'SELECT DISTINCT ad_client_id '
        #                 f'FROM {DATA_CLEANING_SCHEMA}.cleaned_aggregated_data '
        #                 f'ORDER BY ad_client_id')
        # for row in rows:
        #     ad_client_ids.append(row[0])

        state_map = {}
        futures = []
        try:
            # we need to leave half the available cores so that ml-algorithms have some multiprocessing room to breath
            # unfortunately, due to a bug in python runtime https://bugs.python.org/issue38119 and the fact that gridSearchCV uses shared memory,
            # there will be some resource_tracker warnings when de service finishes executing, the warnings pose no real threat,
            # but there is no solution to configure them to be ignored
            with ProcessPoolExecutor(max_workers=round(multiprocessing.cpu_count() / 2)) as executor:
                for ad_client_id in ad_client_ids:
                    cleaned_data = pandas_sql.read_sql(f'SELECT * '
                                                       f'FROM machine_learning.cleaned_aggregated_data '
                                                       f'WHERE AD_Client_ID = {ad_client_id}',
                                                       db_api.db.connect())
                    pipeline_task = executor.submit(run_pipelines, cleaned_data, ad_client_id)
                    futures.append(pipeline_task)
        except Exception as e:
            logger.error(f'Unable to run pipelines executor, err: {str(e)}')
            logger.error(traceback.format_exc())
            state_map[ad_client_id] = False
        wait(futures, timeout=8640, return_when=ALL_COMPLETED)
        for f in futures:
            ad_client_id, result = f.result()
            state_map[ad_client_id] = result

    return state_map


def run_pipelines(data: DataFrame, ad_client_id: int):
    logger.info('Started pipline runner task for client: ' + str(ad_client_id))
    classification_result = False
    regression_result = False
    try:
        with start_transaction(op="classification_pipeline", name="classification_pipeline"):
            classification_result = ClassificationPipeline(data, ad_client_id).run()
    except Exception as e:
        logger.error(f'Unable to generate classification model for the client: {ad_client_id} , err: {str(e)}')
        logger.error(traceback.format_exc())

    try:
        with start_transaction(op="regression_pipeline", name="regression_pipeline"):
            regression_result = RegressionPipeline(data, ad_client_id).run()
    except Exception as e:
        logger.error(f'Unable to generate a regression model for the client: {ad_client_id} , err: {str(e)}')
        logger.error(traceback.format_exc())

    del data

    return ad_client_id, {
        "classification": classification_result,
        'regression': regression_result
    }
