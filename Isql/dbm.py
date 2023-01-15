from typing import List
from dataclasses import make_dataclass
from tqdm import tqdm


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
            def __init__(self, con, cur, origin_where):
                self.con=con
                self.cur=cur
                self.origin_where = origin_where
            def where(self, **condition):
                sql = self.origin_where.where(**condition)
                self.cur.execute(sql)
                self.con.commit()
        return Where(con=self.con, cur=self.cur, origin_where=where)

    
    def query(self, sql):

        class QuerySql:
            def __init__(self, sql):
                self.sql = sql
            def export_data(self):
                sql_clause = self.sql.export_sql()
                print(f'query sql : \n {sql_clause}')
                self.cur.execute(sql_clause)
                return mapping_data()

        def mapping_data(self):
            fields = [field[0] for field in self.cur.description]
            model = make_dataclass('data', fields=fields)     
            models = make_dataclass('dataset', fields=[('data', List)])
            temp = []
            for row in self.cur.fetchall():
                temp.append({fields[idx] : value for idx, value in enumerate(row)})
            dataList = [model(**data) for data in temp]
            temp = None
            return models(data=dataList)
        
        return QuerySql(sql=sql)

