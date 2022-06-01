import logging
from io import StringIO
from typing import Iterator
import pandas.io.sql as pandas_sql
from pandas import DataFrame
import psycopg2
from datasource.db import DB

logger = logging.getLogger('appLog')


class DB_Interface:
    # chunksize of the DataFrame iterator
    __chunksize = 50000

    def __init__(self, db: DB):
        self._db = db

    def get_data_for_pipeline(self,
                              pipeline=None,
                              where_clause: str = None,
                              columns: list = None) -> Iterator[DataFrame]:
        """
        Generic DB data grab
        :param conn:
        :param db:
        :param columns: Columns to be used in the SELECT clause
        :param pipeline: Pipeline enum value
        :param where_clause: Optional SQL condition
        :return:
        """
        connection = self._db.connect()
        columns_to_be_selected = ' * '
        if columns:
            columns_to_be_selected = ' '
            for idx, column in enumerate(columns):
                columns_to_be_selected += column
                if idx == len(columns) - 1:
                    break
                else:
                    columns_to_be_selected += ','

        if pipeline is None:
            raise 'Pipeline Name param must be specified'
        sql = f'SELECT {columns_to_be_selected} FROM {self._db.schema}.{pipeline.source_table_name} '
        if where_clause is not None:
            sql += f' WHERE {where_clause}'
        return pandas_sql.read_sql_query(sql,
                                         connection,
                                         chunksize=DB_Interface.__chunksize,)

    def copy_from_df(self,
                     df: DataFrame,
                     table: str,
                     chunk_size: int = __chunksize,
                     ) -> bool:

        with self._db.connect() as conn:
            with conn.cursor() as cursor:
                try:
                    for i in range(0, df.shape[0], chunk_size):
                        buffer = StringIO()
                        chunk = df.iloc[i:(i + chunk_size)]
                        chunk.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N', quoting=None)
                        buffer.seek(0)
                        cursor.execute(f'SET search_path TO {self._db.schema}')
                        cursor.copy_from(buffer, table, columns=list(df.columns))
                except psycopg2.Error as e:
                    logger.error('COPY FROM Error: ' + str(e))
                    return False

        return True

    def get_data_to_df(self, sql: str = None, params=None):
        return pandas_sql.read_sql(sql, self._db.connect(), params=params)

    def get_data_to_df_iter(self, sql: str = None, params=None) -> Iterator[DataFrame]:
        return pandas_sql.read_sql_query(sql,
                                         self._db.connect(),
                                         chunksize=DB_Interface.__chunksize,
                                         params=params)

    def execute_statement(self, sql: str, params: tuple = None):
        with self._db.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)

    def fetch_one(self, sql: str, params: tuple = None):
        with self._db.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchone()

    def fetch_many(self, sql: str, params: tuple = None):
        with self._db.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()

    @property
    def get_chunksize(self):
        return self.__chunksize
