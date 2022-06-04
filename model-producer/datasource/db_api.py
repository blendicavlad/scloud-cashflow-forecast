from datasource.db import DB


class DB_Interface:

    def __init__(self, db: DB):
        self._db = db

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

