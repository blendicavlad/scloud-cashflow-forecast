from datasource.db import DB
from datasource.db_props import SOURCE_DATABASE, SOURCE_PORT, SOURCE_PASSWORD, SOURCE_HOST, SOURCE_USER, SOURCE_SCHEMA

class SourceDB(DB):

    def __init__(self):
        super().__init__(SOURCE_DATABASE,
                         SOURCE_USER,
                         SOURCE_PASSWORD,
                         SOURCE_HOST,
                         int(SOURCE_PORT),
                         SOURCE_SCHEMA)