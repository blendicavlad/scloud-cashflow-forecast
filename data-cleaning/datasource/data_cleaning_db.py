from datasource.db import DB
from datasource.db_props import DATA_CLEANING_DATABASE, DATA_CLEANING_PORT, DATA_CLEANING_PASSWORD, DATA_CLEANING_HOST, DATA_CLEANING_USER, DATA_CLEANING_SCHEMA

class DataCleaningDB(DB):

    def __init__(self):
        super().__init__(DATA_CLEANING_DATABASE,
                         DATA_CLEANING_USER,
                         DATA_CLEANING_PASSWORD,
                         DATA_CLEANING_HOST,
                         int(DATA_CLEANING_PORT),
                         DATA_CLEANING_SCHEMA)