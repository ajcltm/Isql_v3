from Isql import sqlite, mysql
import sqlite3
import pymysql

def identify_sql(protocol):
    return protocol.split('://')[0]

def get_params(protocol):
    info_part = protocol.split('://')[-1]
    dct = dict()
    for w in info_part.split(';'):
        if w:
            lst = w.split('?')
            dct.update({lst[0]:lst[1]})
    return dct

def create_engine(protocol):    
    sql = identify_sql(protocol)
    params = get_params(protocol)
    print(f'params : {params}')
    return SqlEngine(sql, params)

class SqlEngine:

    def __init__(self, sql, params):
        self.sql = sql
        self.params = params
    
    def get_sql(self):
        option = {'sqlite':sqlite.Sqlite, 'mysql':mysql.Mysql}
        return option.get(self.sql)

    def get_connector(self):
        option = {'sqlite':sqlite3, 'mysql':pymysql}
        return option.get(self.sql).connect(**self.params)
