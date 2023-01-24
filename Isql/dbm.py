from typing import List
from dataclasses import make_dataclass, asdict
from tqdm import tqdm
from Isql import querySql
import pandas as pd
import json


class DBM:

    def __init__(self, connect):
        self.con = connect
        self.cur = self.con.cursor()

    def create_table(self, model, autoIncremnet=None, primaryKey=None, **customType):
        if  customType:
            sql = model.get_create(autoIncremnet, primaryKey, **customType)
        else:
            sql = model.get_create(autoIncremnet, primaryKey)
        self.cur.execute(sql)
        self.con.commit()
    
    def add_primaryKey(self, model, primaryKey):
        sql = model.get_add_primaryKey(primaryKey)
        self.cur.execute(sql)
        self.con.commit()

    def drop_primaryKey(self, model):
        sql = model.get_drop_primaryKey()
        self.cur.execute(sql)
        self.con.commit()
    
    def create_index(self, model, index, unique=None):
        sql = model.get_create_index(index, unique)
        self.cur.execute(sql)
        self.con.commit()
    
    def add_index(self, model, index, unique=None):
        sql = model.get_add_index(index, unique)
        self.cur.execute(sql)
        self.con.commit()

    def drop_index(self, model, index):
        sql = model.get_drop_index(index)
        self.cur.execute(sql)
        self.con.commit()
    
    def drop_table(self, model):
        sql = model.get_drop()
        self.cur.execute(sql)
        self.con.commit()

    def insert_data(self, data, limit=5000):
        def check_dataset(model):
            if not list(model.__fields__.keys())[0] == 'data':
                return False
            if not model.__annotations__.get('data').__name__ == 'List':
                return False
            return True

        def check_scale(dataset):
            return bool(len(dataset.data) > limit)

        def chunk_dataset(dataset):
            model = dataset.__class__
            temp = []
            for i in tqdm(range(0, len(dataset.data), limit), desc='chunking the dataset now...'):
               temp.append(model(data=dataset.data[i:i+limit])) 
            dataset = None
            return temp

        if not check_dataset(model=data.__class__):
            sql = data.get_insert()
            self.cur.execute(sql, tuple(data.dict().values()))
            self.con.commit()
            return

        if not check_scale(dataset=data):
            sql = data.get_insert()
            self.cur.executemany(sql, [tuple(i.dict().values()) for i in data.data])
            self.con.commit()
            return

        for data in chunk_dataset(dataset=data):
            sql = data.get_insert()
            self.cur.executemany(sql, [tuple(i.dict().values()) for i in data.data])
            self.con.commit()
    
    def delete_data(self, model, **condition):
        if condition : 
            sql = model.get_delete(**condition)
        else:
            sql = model.get_delete()
        self.cur.execute(sql)
        self.con.commit()
    
    def update_data(self, model, **value):
        where = model.get_update(**value)
        class Where:
            def __init__(self, con, cur, origin_where, update_condition):
                self.con=con
                self.cur=cur
                self.origin_where = origin_where
                self.update_condition = update_condition
            def where(self, **condition):
                sql = self.origin_where.where(**condition)
                self.update_condition.update(condition)
                values = tuple(self.update_condition.values())
                self.cur.execute(sql, values)
                self.con.commit()
        return Where(con=self.con, cur=self.cur, origin_where=where, update_condition=value)

    
    def query(self, tableName, subQuery=None):

        class InnerQuerySql(querySql.QuerySql):

            def __init__(self, cur, tableName, subQuery=None):
                self.cur = cur
                super().__init__(tableName, subQuery)

            def get_field_list(self):
                return [field[0] for field in self.cur.description]

            def get_dict_list(self, fields):
                temp = []
                for row in self.cur.fetchall():
                    temp.append({fields[idx] : value for idx, value in enumerate(row)})
                return temp

            def get_dataclass_foramt(self, fields, dict_list):
                model = make_dataclass('data', fields=fields)     
                models = make_dataclass('dataset', fields=[('data', List)])
                dataList = [model(**data) for data in dict_list]
                dict_list = None
                return models(data=dataList)

            def export_data(self, format=None):
                sql_clause = self.export_sql()
                print(f'query sql : \n {sql_clause}')
                self.cur.execute(sql_clause)

                if format=='dataclass':
                    fields = self.get_field_list()
                    dict_list = self.get_dict_list(fields=fields)
                    return self.get_dataclass_foramt(fields=fields, dict_list=dict_list)

                if format == 'df':
                    fields = self.get_field_list()
                    dict_list = self.get_dict_list(fields=fields)
                    df = pd.DataFrame(data=dict_list)
                    dict_list = None
                    return df 

                if format == 'json':
                    fields = self.get_field_list()
                    dict_list = self.get_dict_list(fields=fields)
                    json_data = json.dumps({'data':dict_list}, ensure_ascii=False)
                    dict_list = None
                    return json_data

                if format == 'dict':
                    fields = self.get_field_list()
                    dict_list = self.get_dict_list(fields=fields)
                    dict_data = {'data':dict_list}
                    dict_list = None
                    return dict_data

                return list(self.cur.fetchall())
        
        return InnerQuerySql(cur=self.cur, tableName=tableName, subQuery=subQuery)

