class DBM:

    def __init__(self, connect):
        self.con = connect
        self.cur = self.con.cursor()

    def create_table(self, model, **customType):
        if  customType:
            sql = model.get_create(**customType)
        else:
            sql = model.get_create()
        self.cur.execute(sql)
        self.con.commit()
    
    def drop_table(self, model):
        sql = model.get_drop()
        self.cur.execute(sql)
        self.con.commit()

    def insert_data(self, data):
        sql = data.get_insert()
        self.cur.execute(sql)
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
        fields = list(model.__fields__.get('dataset').type_.__fields__.keys()) 
        temp = []
        for row in self.cur.fetchall():
            temp.append({fields[idx] : value for idx, value in enumerate(row)})
        return model(dataset=temp)
