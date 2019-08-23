from ..mysqlmethods import mysqlMethods as mm
from .. import dlaConstants as dlac


class DataEngine(object):
    def __init__(self, corpdb=dlac.DEF_CORPDB, sql_host=dlac.MYSQL_HOST, encoding=dlac.DEF_ENCODING,
                 use_unicode=dlac.DEF_UNICODE_SWITCH, db_type=dlac.DB_TYPE):
        self.encoding = encoding
        self.sql_host = sql_host
        self.corpdb = corpdb
        self.use_unicode = use_unicode
        self.db_type = db_type
        self.dataEngine = None

    def connect(self):
        if self.db_type == "mysql":
            self.dataEngine = MySqlDataEngine(self.corpdb, self.sql_host, self.encoding)
            return self.dataEngine.get_db_connection()
        if self.db_type == "sqlite":
            # instantiate SqliteDataEngine creating the connection with database and return its object
            pass

    def disable_table_keys(self, featureTableName):
        self.dataEngine.disable_table_keys(featureTableName)

    def enable_table_keys(self, featureTableName):
        self.dataEngine.enable_table_keys(featureTableName)

    def execute_get_list(self, usql):
        return self.dataEngine.execute_get_list(usql)



class MySqlDataEngine(DataEngine):

    def __init__(self, corpdb, mysql_host, encoding):
        super().__init__()
        (self.dbConn, self.dbCursor, self.dictCursor) = mm.dbConnect(corpdb, host=mysql_host, charset=encoding)

    def get_db_connection(self):
        return self.dbConn, self.dbCursor, self.dictCursor

    def execute_get_list(self, usql):
        return mm.executeGetList(self.corpdb, self.dbCursor, usql, charset=self.encoding, use_unicode=self.use_unicode)

    def disable_table_keys(self, featureTableName):
        mm.disableTableKeys(self.corpdb, self.dbCursor, featureTableName, charset=self.encoding, use_unicode=self.use_unicode)

    def enable_table_keys(self, featureTableName):
        mm.enableTableKeys(self.corpdb, self.dbCursor, featureTableName, charset=self.encoding, use_unicode=self.use_unicode)

class SqliteDataEngine(DataEngine):
    # contains methods similar to MySqlWrapper class
    # these methods will call methods in mysqliteMethods.py (yet to be created) which will be similar to mysqlMethods.py
    pass
