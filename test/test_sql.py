import unittest
from typing import Optional, List
from datetime import datetime, date
from Isql import sql, sqlite
from pathlib import Path

class Test_1_identify_sql(unittest.TestCase):

    @unittest.skip("")
    def test_1_sqlite_in_the_right_way(self):
        path = str(Path.cwd().joinpath('test', 'test.db'))
        protocol = f'sqlite://database?{path};'
        engine = sql.create_engine(protocol=protocol)

    def test_2_mysql_in_the_right_way(self):
        # pymysql.connect(host='localhost', port=3306, user='root', passwd='2642805', db='fundamentalData', charset='utf8')
        protocol = f'mysql://host?192.168.35.243;port?3306;user?ajcltm;passwd?2642805Ab!;db?test;charset?utf8mb4;'
        engine = sql.create_engine(protocol=protocol)
        con = engine.get_connector()
        
    @unittest.skip("")
    def test_3_sqlite_in_the_wrong_way(self):
        path = str(Path.cwd().joinpath('test', 'test.db'))
        protocol = f'sqlite://db?{path};'
        engine = sql.create_engine(protocol=protocol)
        print(engine.get_sql())
        print(engine.get_connector())

if __name__ == '__main__':
    unittest.main()