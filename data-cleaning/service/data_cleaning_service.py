import os
import traceback
import time
import datetime
from multiprocessing import Queue
from service.pipeline_impl import AggregationPipeline
from datasource.db_api import DB_Interface
from datasource.data_cleaning_db import DataCleaningDB
from datasource.source_db import SourceDB
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, wait, ALL_COMPLETED
import logging
from service.pipeline import Pipeline
from datasource.db_props import SOURCE_SCHEMA, DATA_CLEANING_SCHEMA

logger = logging.getLogger('appLog')


class DataCleaningService:

    def __init__(self):
        logger.info('Data cleaning service started')

        cpus = multiprocessing.cpu_count()
        logger.info('Worker cpus: ' + str(cpus))

        self.data_cleaning_db_api = DB_Interface(DataCleaningDB())
        self.source_db_api = DB_Interface(SourceDB())
        self.workers = cpus if cpus <= len(Pipeline) else len(Pipeline)

    def run(self) -> dict[int, bool]:
        """
        Run a DataCleaning Job for each available CPU core
        """
        ad_client_ids = []
        rows = self.source_db_api\
            .fetch_many(f'SELECT ad_client_id from {SOURCE_SCHEMA}.ad_client order by ad_client_id')
        for row in rows:
            ad_client_ids.append(row[0])
        multiprocess_manager = multiprocessing.Manager()
        queue = multiprocess_manager.Queue()
        state_map = {}
        for client_id in ad_client_ids:
            state_map[client_id] = self.run_for_ad_client(client_id, queue)

        pipeline_state_list = []
        while not queue.empty():
            pipeline_state_list.append(queue.get())

        if pipeline_state_list:
            PipelineState.persist_pipeline_state_list(self.data_cleaning_db_api, pipeline_state_list)
        return state_map

    def run_for_ad_client(self, ad_client_id: int, queue: Queue) -> bool:
        with ProcessPoolExecutor(max_workers=self.workers) as executor:
            futures = []
            for pipeline in Pipeline:
                if pipeline == Pipeline.AGGREGATION:  # aggregation pipeline should run after the rest of pipelines
                    continue
                entity_cleaning_task = executor.submit(self.clean_data,
                                                       pipeline,
                                                       queue,
                                                       ad_client_id)
                futures.append(entity_cleaning_task)
            wait(futures, timeout=1800, return_when=ALL_COMPLETED)
            for f in futures:
                if not f.result():
                    return False

            try:
                logger.info(f'Started aggregation pipeline for client: {ad_client_id}')
                start_time = time.time()

                cleaned_invoices_df = self.data_cleaning_db_api.get_data_to_df(
                    sql=f'SELECT * FROM {DATA_CLEANING_SCHEMA}.c_invoice_cleaned'
                        f' WHERE ad_client_id = ' + str(ad_client_id))
                cleaned_allocations_df = self.data_cleaning_db_api.get_data_to_df(
                    sql=f'SELECT * FROM {DATA_CLEANING_SCHEMA}.c_allocationline_cleaned'
                        f' WHERE ad_client_id = ' + str(ad_client_id))
                cleaned_payments_df = self.data_cleaning_db_api.get_data_to_df(
                    sql=f'SELECT * FROM {DATA_CLEANING_SCHEMA}.c_payment_cleaned'
                        f' WHERE ad_client_id = ' + str(ad_client_id))
                cleaned_terms_df = self.data_cleaning_db_api.get_data_to_df(
                    sql=f'SELECT * FROM {DATA_CLEANING_SCHEMA}.c_paymentterm_cleaned'
                        f' WHERE ad_client_id = ' + str(ad_client_id))

                aggregated_df = AggregationPipeline(ad_client_id) \
                    .run(cleaned_invoices_df, cleaned_allocations_df, cleaned_payments_df, cleaned_terms_df)
                # aggregated_df = pd.DataFrame()
                if aggregated_df is not None and not aggregated_df.empty:
                    self.clear_aggregated_data(ad_client_id)
                    if not self.data_cleaning_db_api.copy_from_df(aggregated_df,
                                                                  AggregationPipeline.dest_table_name):
                        raise Exception(f'Could not persist data into {AggregationPipeline.dest_table_name}')
                    time_run = datetime.timedelta(seconds=(time.time() - start_time))
                    logger.info(f' finished aggregation pipeline for ad_client_id {ad_client_id} in'
                                f' {str(time_run)} seconds')
                    pipeline_state = PipelineState(
                        time_run=time_run,
                        total_rows=len(aggregated_df),
                        result_rows=len(aggregated_df),
                        ad_client_id=ad_client_id,
                        pipeline=Pipeline.AGGREGATION
                    )
                    queue.put(pipeline_state)
                else:
                    logger.error('Could not construct aggregated table')
                    return False
            except Exception as e:
                logger.error(f'Error in aggregation pipeline for client_id: {ad_client_id} \n {str(e)}')
                logger.error(traceback.format_exc())
                return False

            return True

    def clean_data(self,
                   pipeline: Pipeline,
                   queue: Queue,
                   ad_client_id: int) -> bool:
        logger.info(f'Started subprocess with ID:{os.getpid()}'
                    f' for: client: {ad_client_id}'
                    f' ,pipeline: {pipeline.name}')
        try:
            self.process_chunks(pipeline, queue, ad_client_id)
        except Exception as e:
            logger.exception(f'Error in pid: {os.getpid()} '
                             f'for data entity: {pipeline.name} '
                             f'- ERR: {str(e)}')
            logger.error(str(e))
            return False
        return True

    def process_chunks(self,
                       pipeline: Pipeline,
                       queue: Queue,
                       ad_client_id: int):
        """
        Runs data cleaning job in chunks, so we can trade memory for cpu consumption
        :param queue:
        :param ad_client_id:
        :param pipeline:
        """
        start_time = time.time()
        pipeline_obj = pipeline.value(
            ad_client_id)  # Gets the pipeline associated with a data entity, and instantiate it
        where_clause = pipeline_obj.filters
        table_predicate = None

        if pipeline == Pipeline.AGGREGATION:
            table_predicate = 'no_clean_entries_alloc'
        elif pipeline == Pipeline.PAYMENT:
            table_predicate = 'no_clean_entries_payments'
        elif pipeline == Pipeline.INVOICE:
            table_predicate = 'no_clean_entries_invoices'
        elif pipeline == Pipeline.PAYMENT_TERM:
            table_predicate = 'no_clean_entries_terms'

        select = f'SELECT date_run FROM {DATA_CLEANING_SCHEMA}.data_cleaning_client_stats '\
                 ' WHERE ad_client_id=%s '\

        if table_predicate is not None:
            table_predicate = ' AND ' + table_predicate + ' > 0'
            select = select + table_predicate

        select = select + "ORDER BY date_run DESC LIMIT 1"

        date_last_run = self.data_cleaning_db_api.fetch_one(select, (ad_client_id,))

        if date_last_run is not None:
            if where_clause is not None:
                where_clause += " AND "
            where_clause += "created >= to_timestamp('" + str(
                date_last_run[0]) + "', 'YYYY-MM-DD HH24:MI:SS')::timestamp"

        df_iter = self.source_db_api.get_data_for_pipeline(pipeline=pipeline_obj, where_clause=where_clause,
                                                      columns=pipeline_obj.columns_to_use)
        total_chunks, total_rows, result_rows = 0, 0, 0
        dest_table = pipeline.value.dest_table_name

        for chunk in df_iter:
            cleaned_chunk = pipeline_obj.clean_df(chunk)
            result_rows += len(cleaned_chunk)
            total_chunks = total_chunks + 1
            total_rows += len(chunk.index)
            if not self.data_cleaning_db_api.copy_from_df(df=cleaned_chunk,
                                             table=dest_table):
                raise Exception(f'Could not persist data into {dest_table}')
        time_run = datetime.timedelta(seconds=(time.time() - start_time))
        logger.info(f'Processed {total_chunks} chunks'
                    f' for a total of {total_rows} rows'
                    f' for data entity {pipeline.name}'
                    f' with a result of {result_rows} remaining rows'
                    f' finished in {str(time_run)} seconds')
        del chunk
        pipeline_state = PipelineState(
            time_run=time_run,
            total_rows=total_rows,
            result_rows=result_rows,
            ad_client_id=ad_client_id,
            pipeline=pipeline
        )
        del df_iter
        queue.put(pipeline_state)

    def clear_aggregated_data(self, ad_client_id: int):
        self.data_cleaning_db_api.execute_statement(
            f'DELETE FROM {DATA_CLEANING_SCHEMA}.cleaned_aggregated_data WHERE AD_Client_ID = %s',
            (ad_client_id,))


class PipelineState:

    def __init__(self, time_run, total_rows, result_rows, ad_client_id, pipeline):
        self.time_run = time_run
        self.total_rows = total_rows
        self.result_rows = result_rows
        self.ad_client_id = ad_client_id
        self.pipeline = pipeline

    @staticmethod
    def persist_pipeline_state_list(db_api: DB_Interface, state_list: list):
        state_dict = {}
        for state in state_list:
            state_dict.setdefault(state.ad_client_id, []).append(state)
        for k, v in state_dict.items():
            client_state = {
                'ad_client_id': k,
                'runtime_in_seconds': 0,
                'no_entries_invoices': 0,
                'no_entries_alloc': 0,
                'no_entries_payments': 0,
                'no_entries_terms': 0,
                'no_clean_entries_invoices': 0,
                'no_clean_entries_alloc': 0,
                'no_clean_entries_payments': 0,
                'no_clean_entries_terms': 0,
                'invoices_remaining_percent': 0,
                'alloc_remaining_percent': 0,
                'payments_remaining_percent': 0,
                'terms_remaining_percent': 0,
                'avg_remaining_percent': 0,
                'no_total_aggregated_rows': 0
            }
            for pipeline_result in v:
                client_state['runtime_in_seconds'] = client_state[
                                                         'runtime_in_seconds'] +\
                                                     pipeline_result.time_run.total_seconds()
                if pipeline_result.pipeline == Pipeline.INVOICE:
                    client_state['no_entries_invoices'] = pipeline_result.total_rows
                    client_state['no_clean_entries_invoices'] = pipeline_result.result_rows
                    if pipeline_result.result_rows != 0:
                        client_state['invoices_remaining_percent'] = \
                            (pipeline_result.result_rows / pipeline_result.total_rows) * 100
                elif pipeline_result.pipeline == Pipeline.ALLOCATION:
                    client_state['no_entries_alloc'] = pipeline_result.total_rows
                    client_state['no_clean_entries_alloc'] = pipeline_result.result_rows
                    if pipeline_result.result_rows != 0:
                        client_state['alloc_remaining_percent'] = \
                            (pipeline_result.result_rows / pipeline_result.total_rows) * 100
                elif pipeline_result.pipeline == Pipeline.PAYMENT_TERM:
                    client_state['no_entries_terms'] = pipeline_result.total_rows
                    client_state['no_clean_entries_terms'] = pipeline_result.result_rows
                    if pipeline_result.result_rows != 0:
                        client_state['terms_remaining_percent'] = \
                            ( pipeline_result.result_rows / pipeline_result.total_rows) * 100
                elif pipeline_result.pipeline == Pipeline.PAYMENT:
                    client_state['no_entries_payments'] = pipeline_result.total_rows
                    client_state['no_clean_entries_payments'] = pipeline_result.result_rows
                    if pipeline_result.result_rows != 0:
                        client_state['payments_remaining_percent'] = \
                            (pipeline_result.result_rows / pipeline_result.total_rows) * 100
                elif pipeline_result.pipeline == Pipeline.AGGREGATION:
                    client_state['no_total_aggregated_rows'] = pipeline_result.result_rows
            client_state['avg_remaining_percent'] = (client_state['invoices_remaining_percent'] +
                                                     client_state['alloc_remaining_percent'] +
                                                     client_state['terms_remaining_percent'] +
                                                     client_state['payments_remaining_percent']) / 4

            insert = f'INSERT INTO machine_learning.data_cleaning_client_stats ' \
                     f'(ad_client_id, runtime_in_seconds, no_entries_invoices,' \
                     f' no_entries_alloc, no_entries_payments, no_entries_terms,' \
                     f' no_clean_entries_invoices, no_clean_entries_alloc, no_clean_entries_payments,' \
                     f' no_clean_entries_terms, invoices_remaining_percent, alloc_remaining_percent,' \
                     f' payments_remaining_percent, terms_remaining_percent, avg_remaining_percent, no_total_aggregated_rows) ' \
                     f' VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

            db_api.execute_statement(insert, tuple(client_state.values()))
            logger.info("Inserted statistics for ad_client_id: " + str(k))
            logger.info(str(client_state))
