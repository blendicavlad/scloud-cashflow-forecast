from psycopg2.extensions import STATUS_BEGIN

from datasource.db import DB
from datasource.db_props import DATA_CLEANING_USER, DATA_CLEANING_PASSWORD, DATA_CLEANING_DATABASE, DATA_CLEANING_HOST, \
    DATA_CLEANING_PORT, DATA_CLEANING_SCHEMA


class DB_Interface:

    def __init__(self, db: DB = None):
        self._db = db

    def __enter__(self):
        if self._db is None:
            self._db = DB(DATA_CLEANING_DATABASE,
                          DATA_CLEANING_USER,
                          DATA_CLEANING_PASSWORD,
                          DATA_CLEANING_HOST,
                          DATA_CLEANING_PORT,
                          DATA_CLEANING_SCHEMA)
        self._db.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._db.connection is not None and self._db.connection.status == STATUS_BEGIN:
            if exc_type is None:
                self._db.connection.commit()
            else:
                self._db.connection.rollback()

        self._db.close()

    @property
    def db(self):
        return self._db

    def execute_statement(self, sql: str, params: tuple = None):
        with self._db.cursor() as cursor:
            cursor.execute(sql, params)

    def fetch_one(self, sql: str, params: tuple = None):
        with self._db.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone()

    def fetch_many(self, sql: str, params: tuple = None):
        with self._db.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()
