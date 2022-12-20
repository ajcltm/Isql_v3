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

    def insert_data(self, data):
        def check_dataset(model):
            if not list(model.__fields__.keys())[0] == 'data':
                return False
            if not model.__annotations__.get('data').__name__ == 'List':
                return False
            return True

        def check_scale(dataset):
            return bool(len(dataset.data) > 5000)

        def chunk_dataset(dataset):
            model = dataset.__class__
            temp = []
            for i in tqdm(range(0, len(dataset.data), 5000), desc='chunking the dataset now...'):
               temp.append(model(data=dataset.data[i:i+5000])) 
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

    def query_data(self, model, sql):
        self.cur.execute(sql)
        fields = list(model.__fields__.get('data').type_.__fields__.keys()) 
        temp = []
        for row in self.cur.fetchall():
            temp.append({fields[idx] : value for idx, value in enumerate(row)})
        return model(data=temp)
