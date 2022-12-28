import psycopg2
import logging

logger = logging.getLogger('modelConsumerLog')

LIMIT_RETRIES = 3

class DBConnectionException(Exception):
    def __init__(self):
        super().__init__('Database connection error')
    pass

class DB:

    def __init__(self,
                 database,
                 user,
                 password,
                 host,
                 port,
                 schema):
        self.__connection = None
        self.__cursor = None
        self.__database = database
        self.__user = user
        self.__password = password
        self.__host = host
        self.__port = port
        self.__schema = schema

    def connected(self) -> bool:
        return self.__connection and self.__connection.closed == 0

    def connect(self, retry_counter=0):
        if not self.__connection:
            try:
                self.__connection = psycopg2.connect(dbname=self.__database,
                                                     user=self.__user,
                                                     password=self.__password,
                                                     host=self.__host,
                                                     port=self.__port,
                                                     connect_timeout=5)
                retry_counter = 0
            except psycopg2.OperationalError as err:
                if retry_counter >= LIMIT_RETRIES:
                    raise err
                else:
                    retry_counter += 1
                    logger.error("got error: {}. reconnecting {}".format(str(err).strip(), retry_counter))
                    return self.connect(retry_counter)
            except (Exception, psycopg2.Error) as err:
                logger.error(err)
                raise DBConnectionException()
        return self.__connection

    def cursor(self):
        if not self.__cursor or self.__cursor.closed:
            if not self.__connection:
                self.connect()
            self.__cursor = self.__connection.cursor()
            return self.__cursor

    def close(self):
        if self.connected():
            try:
                if self.__cursor:
                    self.__cursor.close()
                self.__connection.close()
            except Exception as e:
                logger.error('unable to close db connection: ' + str(e))
                pass
        self.__connection = None

    @property
    def connection(self):
        return self.__connection

    @property
    def schema(self):
        return self.__schema
