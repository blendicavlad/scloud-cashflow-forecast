import logging
import traceback

from pandas import DataFrame
from datasource.db import DB
from datasource.db_api import DB_Interface
from datasource.db_props import DATA_CLEANING_USER,DATA_CLEANING_PASSWORD,DATA_CLEANING_DATABASE,DATA_CLEANING_HOST,DATA_CLEANING_PORT,DATA_CLEANING_SCHEMA
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, wait, ALL_COMPLETED
from .classification_pipeline import ClassificationPipeline
from .regression_pipeline import RegressionPipeline
import pandas.io.sql as pandas_sql

logger = logging.getLogger('modelProducerLog')

class PipelineRunner:

    def __init__(self):
        self.__db = DB(DATA_CLEANING_DATABASE,
                       DATA_CLEANING_USER,
                       DATA_CLEANING_PASSWORD,
                       DATA_CLEANING_HOST,
                       DATA_CLEANING_PORT,
                       DATA_CLEANING_SCHEMA)
        self.__db_api = DB_Interface(self.__db)

    def run(self):
        ad_client_ids = []
        rows = self.__db_api\
            .fetch_many(f'SELECT DISTINCT ad_client_id from {DATA_CLEANING_SCHEMA}.cleaned_aggregated_data order by ad_client_id')
        for row in rows:
            ad_client_ids.append(row[0])

        state_map = {}
        for ad_client_id in ad_client_ids:
            cleaned_data = pandas_sql.read_sql(f'SELECT * '
                                          f'FROM machine_learning.cleaned_aggregated_data '
                                          f'WHERE AD_Client_ID = {ad_client_id}',
                             self.__db.connect())
            try :
                state_map[ad_client_id] = PipelineRunner.__run_pipelines(cleaned_data, ad_client_id)
            except Exception as e:
                logger.error(f'Unable to run pipelines for client: {ad_client_id} , err: {str(e)}')
                logger.error(traceback.format_exc())
                state_map[ad_client_id] = False
                continue
        self.__db.close()

        return state_map

    @staticmethod
    def __run_pipelines(data: DataFrame, ad_client_id: int):
        # with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        #     futures = []
        #     classification_pipeline = ClassificationPipeline(data, ad_client_id)
        #     regression_pipeline = RegressionPipeline(data, ad_client_id)
        #     classification_task = executor.submit(classification_pipeline.run)
        #     futures.append(classification_task)
        #     regression_task = executor.submit(regression_pipeline.run)
        #     futures.append(regression_task)
        #     wait(futures, timeout=1800, return_when=ALL_COMPLETED)
        #     for f in futures:
        #         if not f.result():
        #             return False
        classification_pipeline = ClassificationPipeline(data, ad_client_id).run()
        regression_pipeline = RegressionPipeline(data, ad_client_id).run()
        return classification_pipeline and regression_pipeline


