import unittest
from typing import Optional, List
from datetime import date, datetime
from Isql import sql, DBM
from pathlib import Path


# _dir = str(Path.cwd().joinpath('test', 'test.db'))
# protocol = f'sqlite://database?{_dir};'
# engine = sql.create_engine(protocol)
# _sql = engine.get_sql()

protocol = f'mysql://host?192.168.35.243;port?3306;user?ajcltm;passwd?2642805Ab!;db?test;charset?utf8mb4;'
engine = sql.create_engine(protocol)
_sql = engine.get_sql()

class TestModel(_sql):
    attr_1 : str
    attr_2 : int
    attr_3 : float
    attr_4 : date
    attr_5 : datetime
    attr_6 : Optional[str]

class TestModels(_sql):
    data : List[TestModel]

class TestModelAttr(_sql):
    model : str
    attr_some : str


class Test_1_dbm(unittest.TestCase):

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
            'attr_5' : datetime(1988, 9, 29, 12, 10, 1),
            'attr_6' : 'something'
        }

        self.raw_data3 = {
            'attr_1' : 'dongi',
            'attr_2' : 200,
            'attr_3' : 3.14,
            'attr_4' : datetime(1923, 8, 29).date(),
            'attr_5' : datetime(1988, 9, 29, 12, 10, 1),
            'attr_6' : "I'm dongi" 
        }

        self.raw_data4 = {
            'attr_1' : 'dongre',
            'attr_2' : 200,
            'attr_3' : 3.14,
            'attr_4' : datetime(1923, 8, 29).date(),
            'attr_5' : datetime(1988, 9, 29, 12, 10, 1),
            'attr_6' : '\, ", (, ), %, &, @, *, [, ], {, }, ^, !, /, -, +, ?, ;, ~, |'
        }

        self.data1 = TestModel(**self.raw_data1)
        self.data2 = TestModel(**self.raw_data2)
        self.data3 = TestModel(**self.raw_data3)
        self.data4 = TestModel(**self.raw_data4)
        self.dataset = TestModels(data=[self.data1, self.data2, self.data3, self.data4])

        
        self.db = DBM(engine.get_connector())
    
    @unittest.skip('for some reason')
    def test_1_create_table_default_type_and_drop(self):
        self.db.create_table(model=TestModel)
        self.db.drop_table(model=TestModel)
    
    @unittest.skip('for some reason')
    def test_2_create_table_customType_and_drop(self):
        self.db.create_table(model=TestModel, attr_1='varchar(100)', attr_2='tinyint')
        self.db.drop_table(model=TestModel)

    def test_3_insert_data(self):
        self.db.create_table(model=TestModel)
        self.db.insert_data(data=self.data1)
        self.db.insert_data(data=self.data2)
        self.db.insert_data(data=self.data3)
        self.db.insert_data(data=self.data4)
        self.db.insert_data(data=self.dataset)
    
    @unittest.skip('for some reason')
    def test_4_insert_large_scale_data(self):
        def create_data(num):
            raw_data = {
                'attr_1' : f'{num}',
                'attr_2' : 200,
                'attr_3' : 3.14,
                'attr_4' : datetime(1923, 8, 29).date(),
                'attr_5' : datetime(1988, 9, 29, 12, 10, 1),
                'attr_6' : 'something'
            }
            return raw_data

        self.db.create_table(model=TestModels)
        large_dataset = TestModels(data=[create_data(i) for i in range(30000)])
        self.db.insert_data(data=large_dataset, limit=30002)


    @unittest.skip('for some reason')
    def test_5_delete_data_in_some_condition(self):
        self.db.delete_data(model=TestModels, attr_1='kim')
    
    @unittest.skip('for some reason')
    def test_6_delete_data(self):
        self.db.delete_data(model=TestModels)

    def test_7_query_data(self):
        data = self.db.query_data(sql='select * from TestModel')
        print(data)

    @unittest.skip('for some reason')
    def test_8_update_data(self):
       self.db.create_table(model=TestModel) 
       self.db.delete_data(model=TestModel)
       self.db.insert_data(data=self.dataset)
       self.db.update_data(model=TestModels, attr_6='Not Null Value').where(attr_1='kim')
       data = self.db.query_data(model=TestModels, sql='select * from TestModel')
       print(data)

    @unittest.skip('for some reason')
    def test_9_model_attr(self):
        self.db.create_table(model=TestModelAttr, _model_='varchar(100)')
        self.db.create_table(model=TestModelAttr, __model__='varchar(100)')
        self.db.create_table(model=TestModelAttr, model_='varchar(100)')
        self.db.create_table(model=TestModelAttr, __model='varchar(100)')

    @unittest.skip('for some reason')
    def test_10_primaryKey(self):
        # self.db.create_table(model=TestModel, primaryKey='attr_1')
        self.db.create_table(model=TestModel, primaryKey=['attr_1', 'attr_2'])
        # self.db.create_table(model=TestModels)
        # self.db.add_primaryKey(model=TestModels, primaryKey=['attr_1', 'attr_3'])
        # self.db.drop_primaryKey(model=TestModel)
    
    @unittest.skip('for some reason')
    def test_11_auto_increment(self):
        self.db.create_table(model=TestModel, autoIncremnet=['attr_1', 'attr_2'])
        # self.db.create_table(model=TestModel, autoIncrement=['attr_1', 'attr_2'], pri ['attr_1','attr_2'])
        self.db.create_table(model=TestModels, autoIncremnet=['attr_1', 'attr_2'], primaryKey=['attr_1', 'attr_3'], attr_1='integer' ,attr_5='varchar(100)')
    
    @unittest.skip('for some reason')
    def test_12_index(self):
        self.db.create_table(model=TestModels)
        self.db.create_index(model=TestModels, index='attr_2', unique=True)
        # self.db.create_index(model=TestModels, index=['attr_2', 'attr_4'])
        # self.db.add_index(model=TestModels, index=['attr_1'])
        # self.db.drop_index(model=TestModels, index='attr_2')

if __name__ == '__main__':
    unittest.main()