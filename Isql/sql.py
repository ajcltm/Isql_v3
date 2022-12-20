from pydantic import BaseModel
from datetime import datetime, date


class TableIdentifier:

    def check_dataset_model(self, model):
        if not list(model.__fields__.keys())[0] == 'data':
            return False
        if not model.__annotations__.get('data').__name__ == 'List':
            return False
        return True

class TableNameExporter(TableIdentifier):

    def get_table_name(self, model):
        if self.check_dataset_model(model):
            table_name = model.__fields__.get('data').type_.__name__
        else:
            table_name= model.__name__
        return table_name

class TableFieldsInfoExporter(TableIdentifier):

    def replace_type(self, model):
        replace = {str:'text', int:'integer', float:'real', datetime:'datetime', date:'date'}
        return {key : replace.get(value.type_) for key, value in model.__fields__.items()}

    def get_fields_info(self, model):
        if self.check_dataset_model(model):
            fields_info = self.replace_type(model=model.__fields__.get('data').type_) 
        else:
            fields_info = self.replace_type(model=model)
        return fields_info

class CreateDropSql(BaseModel):

    @staticmethod
    def handle_duplicated_attr(**customType):
        return {key.strip('_'):value for key, value in customType.items()}

    @staticmethod
    def add_property(fields_info, fields, property):
        if isinstance(fields, list):
            return {field : f'{fields_info.get(field)} {property}' for field in fields}
        return {fields : f'{fields_info.get(fields)} {property}'}

    @classmethod
    def get_create(cls, autoIncrement=None, primaryKey=None, **customType):
        table_name= TableNameExporter().get_table_name(model=cls)
        fields_info = TableFieldsInfoExporter().get_fields_info(model=cls)
        customType=cls.handle_duplicated_attr(**customType)
        fields_info.update(customType)
        if autoIncrement:
            autoIncrement_dict = cls.add_property(fields_info, autoIncrement, 'AUTO_INCREMENT')
            fields_info.update(autoIncrement_dict)
        type_part = ', '.join([f'{field} {type_}' for field, type_ in fields_info.items()])
        primaryKey = cls.handle_list(primaryKey)
        if primaryKey:
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({type_part}, PRIMARY KEY ({primaryKey}))"
        else:
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({type_part})"
        print(f'create sql : \n {sql}')
        return sql
    
    @classmethod
    def get_add_primaryKey(cls, primaryKey):
        table_name= TableNameExporter().get_table_name(model=cls)
        primaryKey = cls.handle_list(primaryKey)
        sql = f'ALTER TABLE {table_name} ADD PRIMARY KEY ({primaryKey})'
        print(f'primaryKey sql : \n {sql}')
        print(f'Warning! : This method can be used only for MySql not Sqlite')
        return sql
    
    @classmethod
    def get_drop_primaryKey(cls):
        table_name= TableNameExporter().get_table_name(model=cls)
        sql = f'ALTER TABLE {table_name} DROP PRIMARY KEY'
        print(f'primaryKey sql : \n {sql}')
        print(f'Warning! : This method can be used only for MySql not Sqlite')
        return sql

    @classmethod
    def get_drop(cls):
        table_name = TableNameExporter().get_table_name(model=cls) 
        sql = f'DROP TABLE {table_name}'
        print(f'drop sql : \n {sql}')
        return sql
    
class IndexSql:

    @staticmethod
    def handle_list(value):
        if isinstance(value, list):
            return ', '.join(value)
        return value

    @staticmethod
    def get_index_name(value):
        if isinstance(value, list):
            return 'index_'+'_'.join(value)
        return f'index_{value}'

    @classmethod
    def get_create_index(cls, index, unique=None):
        table_name= TableNameExporter().get_table_name(model=cls)
        index_name = cls.get_index_name(index)
        index = cls.handle_list(index)
        if unique:
            sql = f'CREATE UNIQUE INDEX IF NOT EXISTS {index_name} on {table_name} ({index})'
        else:
            sql = f'CREATE INDEX IF NOT EXISTS {index_name} on {table_name} ({index})'
        print(f'index sql : \n {sql}')
        return sql

    @classmethod
    def get_add_index(cls, index, unique=None):
        table_name= TableNameExporter().get_table_name(model=cls)
        index_name = cls.get_index_name(index)
        index = cls.handle_list(index)
        if unique:
            sql = f'ALTER TABLE {table_name} ADD UNIQUE INDEX IF NOT EXISTS {index_name} ({index})'
        else:
            sql = f'ALTER TABLE {table_name} ADD INDEX IF NOT EXISTS {index_name} ({index})'
        print(f'index sql : \n {sql}')
        print(f'Warning! : This method can be used only for MySql not Sqlite')
        return sql
        
    @classmethod
    def get_drop_index(cls, index):
        table_name= TableNameExporter().get_table_name(model=cls)
        index_name = cls.get_index_name(index)
        sql = f'DROP INDEX {index_name} ON {table_name}'
        print(f'index sql : \n {sql}')
        return sql


class InsertSql:

    def get_values_part(self):
        data = self.dict().values()
        values_part_lst = [stringfy(value) for value in data]
        values_part = ','.join(values_part_lst)
        return f'({values_part})'

    def get_values_parts(self):
        values_part_lst = [data.get_values_part() for data in self.data]
        values_part = ', '.join(values_part_lst)
        return values_part

    def get_insert(self):
        table_name = TableNameExporter().get_table_name(model = self.__class__)
        fields = list(TableFieldsInfoExporter().get_fields_info(model=self.__class__).keys())
        if TableIdentifier().check_dataset_model(model=self.__class__):
            data = tuple(self.data[0].dict().values())
        else:
            data = tuple(self.dict().values()) 
        fields_part = ', '.join(fields)
        q_marks = '('+','.join(f'?'*len(data))+')'
        q_marks = '('+','.join(['%s' for i in range(0, len(data))])+')'
        sql = f'INSERT INTO {table_name} ({fields_part}) VALUES {q_marks}' 
        print(f'insert sql : \n {sql[:1000]}')
        return sql

class DeleteSql:

    @classmethod
    def get_delete(cls, **condition):
        table_name = TableNameExporter().get_table_name(model=cls)
        ls = [f"{key} = '{value}'" for key, value in condition.items()]
        if ls:
            sql = f'delete from {table_name} where ' + ' and '.join(ls)
        else:
            sql = f'delete from {table_name}'
        print(f'delete sql : \n{sql}')
        return sql

class UpdateSql:

    @classmethod
    def get_update(cls, **values):
        def get_value_part(**values):
            ls = [f"{key}='{value}'" for key, value in values.items()]
            value_part = 'SET ' + ', '.join(ls)
            return value_part
        table_name = TableNameExporter().get_table_name(model=cls)
        value_part = get_value_part(**values)
        sql = f"UPDATE {table_name} {value_part}"
        return cls.Where(sql) 

    class Where:
        def __init__(self, sql):
            self.sql = sql
        def where(self, **arg):
            ls = [f"{key}='{value}'" for key, value in arg.items()]
            where_part = 'WHERE ' + ' and '.join(ls)
            sql = f"{self.sql} {where_part}"
            print(f'update sql : \n {sql}')
            return sql

class Sql(CreateDropSql, IndexSql, InsertSql, DeleteSql, UpdateSql):
    pass
