import logging
from io import StringIO
from typing import Iterator
import pandas.io.sql as pandas_sql
from pandas import DataFrame
import psycopg2

from datasource.db_props import SCHEMA

from datasource.db import DB

logger = logging.getLogger('appLog')


class DB_Interface:
    # chunksize of the DataFrame iterator
    __chunksize = 50000
    __db = DB()

    @staticmethod
    def get_data_for_pipeline(pipeline=None,
                              where_clause: str = None,
                              columns: list = None,
                              conn=None) -> Iterator[DataFrame]:
        """
        Generic DB data grab
        :param conn:
        :param db:
        :param columns: Columns to be used in the SELECT clause
        :param pipeline: Pipeline enum value
        :param where_clause: Optional SQL condition
        :return:
        """
        connection = conn
        if connection is None:
            connection = DB_Interface.__get_db_connection()
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
        sql = f'SELECT {columns_to_be_selected} FROM {SCHEMA}.{pipeline.initial_table_name} '
        if where_clause is not None:
            sql += f' WHERE {where_clause}'
        return pandas_sql.read_sql_query(sql,
                                         connection,
                                         chunksize=DB_Interface.__chunksize, )

    @staticmethod
    def copy_from_df(df: DataFrame,
                     table: str,
                     cursor,
                     chunk_size: int = __chunksize,
                     ) -> bool:

        try:
            for i in range(0, df.shape[0], chunk_size):
                buffer = StringIO()
                chunk = df.iloc[i:(i + chunk_size)]
                chunk.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N', quoting=None)
                buffer.seek(0)
                cursor.execute(f'SET search_path TO {SCHEMA}')
                cursor.copy_from(buffer, table, columns=list(df.columns))
        except psycopg2.Error as e:
            logger.error('COPY FROM Error: ' + str(e))
            return False

        return True

    @staticmethod
    def __get_db_connection():
        if not DB_Interface.__db.connected():
            DB_Interface.__db.connect()
        return DB_Interface.__db.connection

    @staticmethod
    def get_data_to_df(sql: str = None, params=None, conn=None):
        if conn is None:
            conn = DB_Interface.__get_db_connection()
        return pandas_sql.read_sql(sql, conn, params=params)

    @staticmethod
    def get_data_to_df_iter(sql: str = None, params=None, conn=None) -> Iterator[DataFrame]:
        if conn is None:
            conn = DB_Interface.__get_db_connection()
        return pandas_sql.read_sql_query(sql,
                                         conn,
                                         chunksize=DB_Interface.__chunksize,
                                         params=params)

    @staticmethod
    def execute_statement(sql: str, params: tuple = None, conn=None):
        if conn is None:
            conn = DB_Interface.__get_db_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)

    @staticmethod
    def fetch_one(sql: str, params: tuple = None, conn=None):
        if conn is None:
            conn = DB_Interface.__get_db_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchone()

    @staticmethod
    def fetch_many(sql: str, params: tuple = None, conn=None):
        if conn is None:
            conn = DB_Interface.__get_db_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()

    @property
    def get_chunksize(self):
        return self.__chunksize
