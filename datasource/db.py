import psycopg2
from datasource.db_props import USER, PORT, PASSWORD, HOST, DATABASE


class DB:

    def __init__(self):
        self._connection = None

    def connected(self) -> bool:
        return self._connection and self._connection.closed == 0

    def connect(self):
        self.close()
        self._connection = psycopg2.connect(dbname=DATABASE,
                                            user=USER,
                                            password=PASSWORD,
                                            host=HOST,
                                            port=PORT)

    def close(self):
        if self.connected():
            try:
                self._connection.close()
            except Exception:
                pass

        self._connection = None

    @property
    def connection(self):
        return self._connection

    @staticmethod
    def get_isolated_connection():
        return psycopg2.connect(dbname=DATABASE,
                                user=USER,
                                password=PASSWORD,
                                host=HOST,
                                port=PORT)
