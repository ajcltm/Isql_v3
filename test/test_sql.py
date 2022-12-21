import unittest
from typing import Optional, List
from datetime import datetime, date
from Isql import sql, sqlite
from pathlib import Path

class Table(sqlite.Sqlite):
    attr_1 : str
    attr_2 : int
    attr_3 : float
    attr_4 : date
    attr_5 : datetime
    attr_6 : Optional[str]

class Tables(sqlite.Sqlite):
    data : List[Table]

# class Test_2_CreateDropSql(unittest.TestCase):

#     def setUp(self) -> None:
        
#         self.raw_data1 = {
#             'attr_1' : 'kim',
#             'attr_2' : 100,
#             'attr_3' : 3.14,
#             'attr_4' : datetime(1923, 8, 29).date(),
#             'attr_5' : datetime(1988, 9, 29)
#         }

#         self.raw_data2 = {
#             'attr_1' : 'dongi',
#             'attr_2' : 200,
#             'attr_3' : 3.14,
#             'attr_4' : datetime(1923, 8, 29).date(),
#             'attr_5' : datetime(1988, 9, 29),
#             'attr_6' : 'something'
#         }
#         self.data1 = Table(**self.raw_data1)
#         self.data2 = Table(**self.raw_data2)
#         self.dataset = Tables(data=[self.data1, self.data2])


#     def test_1_get_create_default(self):
#         Table.get_create()
#         Tables.get_create()
    
#     def test_2_get_create_custom_type(self):
#         Table.get_create(attr_1='varchar(100)', attr_2='tinyint')
#         Tables.get_create(attr_1='varchar(100)', attr_2='tinyint')

#     def test_2_get_drop(self):
#         Table.get_drop()
#         Tables.get_drop()
    
#     def test_3_get_insert(self):
#         self.data1.get_insert()
#         self.data2.get_insert()
#         self.dataset.get_insert()

class Test_3_identify_sql(unittest.TestCase):

    def test_1_identify_sql(self):
        protocol = 'sqlite://something'
        sql_ = sql.identify_sql(protocol=protocol)
        self.assertEqual('sqlite', sql_)

    def test_2_identify_sql(self):
        path = str(Path.cwd().joinpath('test', 'test.db'))
        protocol = f'sqlite://database?{path};'
        engine = sql.create_engine(protocol=protocol)
        print(engine.get_sql())
        print(engine.get_connector())

        protocol = f'mysql://username?ajcltm;passward?1234;dbname?test'
        engine = sql.create_engine(protocol=protocol)
        


if __name__ == '__main__':
    unittest.main()