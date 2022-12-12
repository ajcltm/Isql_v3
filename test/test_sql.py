import unittest
from typing import Optional, List
from datetime import datetime, date
from Isql import sql

class Test_1_stringfy_mysql(unittest.TestCase):

    def test_string_with_comma(self):

        test_string_1 = "'x'"
        processed_1 = sql.stringfy(test_string_1)
        self.assertEqual("'''x'''", processed_1)  # -> ''\x\''

        test_string_2 = '"y"'
        processed_2 = sql.stringfy(test_string_2)
        answer = '''\\"y\\"''' # -> "\y\"
        self.assertEqual(f"'{test_string_2}'", processed_2) # -> '"\y\"'

    def test_datetime_to_sql_style(self):

        test_date_1 = datetime(1923, 8, 29)
        processed_1 = sql.stringfy(test_date_1)
        self.assertEqual("'1923-08-29 00:00:00'", processed_1)

        test_date_2 = date(1923, 8, 29)
        processed_2 = sql.stringfy(test_date_2)
        self.assertEqual("'1923-08-29'", processed_2)
        
    def test_none_value(self):
        test_value = None
        processed = sql.stringfy(test_value)
        self.assertEqual('Null', processed) # -> Null

    def test_number_value(self):
        test_value = 1234
        processed = sql.stringfy(test_value)
        self.assertEqual('1234', processed) # -> 1234

    def test_usual_words(self):
        test_word = 'kim'
        processed = sql.stringfy(test_word)
        self.assertEqual("'kim'", processed) # -> 'kim'

class Table(sql.Sql):
    attr_1 : str
    attr_2 : int
    attr_3 : float
    attr_4 : date
    attr_5 : datetime
    attr_6 : Optional[str]

class Tables(sql.Sql):
    data : List[Table]

class Test_2_CreateDropSql(unittest.TestCase):

    def setUp(self) -> None:
        
        self.raw_data1 = {
            'attr_1' : 'kim',
            'attr_2' : 100,
            'attr_3' : 3.14,
            'attr_4' : datetime(1923, 8, 29).date(),
            'attr_5' : datetime(1988, 9, 29)
        }

        self.raw_data2 = {
            'attr_1' : 'dongi',
            'attr_2' : 200,
            'attr_3' : 3.14,
            'attr_4' : datetime(1923, 8, 29).date(),
            'attr_5' : datetime(1988, 9, 29),
            'attr_6' : 'something'
        }
        self.data1 = Table(**self.raw_data1)
        self.data2 = Table(**self.raw_data2)
        self.dataset = Tables(data=[self.data1, self.data2])


    def test_1_get_create_default(self):
        Table.get_create()
        Tables.get_create()
    
    def test_2_get_create_custom_type(self):
        Table.get_create(attr_1='varchar(100)', attr_2='tinyint')
        Tables.get_create(attr_1='varchar(100)', attr_2='tinyint')

    def test_2_get_drop(self):
        Table.get_drop()
        Tables.get_drop()
    
    def test_3_get_insert(self):
        self.data1.get_insert()
        self.data2.get_insert()
        self.dataset.get_insert()



if __name__ == '__main__':
    unittest.main()