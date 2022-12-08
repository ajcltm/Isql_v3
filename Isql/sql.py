from pydantic import BaseModel
from datetime import datetime, date

def stringfy(value:any)->str:
    for_symbol = ["'", '(', ')', '%', '<', '>']
    if type(value) == str:
        for i in for_symbol:
            value = value.replace(i, '\\'+i)
        value = value.replace('"', '""')
        return f'"{value}"'

    elif type(value)==datetime :
        value = value.strftime(format='%Y-%m-%d-%H-%M-%S')
        return f'"{value}"'

    elif type(value)==date :
        value = value.strftime(format='%Y-%m-%d')
        return f'"{value}"'

    elif value == None:
        return "Null"

    else:
        return f'{value}'


class TableIdentifier:

    def check_dataset_model(self, model):
        if not list(model.__fields__.keys())[0] == 'dataset':
            return False
        if not model.__annotations__.get('dataset').__name__ == 'List':
            return False
        return True

class TableNameExporter(TableIdentifier):

    def get_table_name(self, model):
        if self.check_dataset_model(model):
            table_name = model.__fields__.get('dataset').type_.__name__
        else:
            table_name= model.__name__
        return table_name

class TableFieldsInfoExporter(TableIdentifier):

    def replace_type(self, model):
        replace = {str:'text', int:'integer', float:'real', datetime:'date', date:'date'}
        return {key : replace.get(value.type_) for key, value in model.__fields__.items()}

    def get_fields_info(self, model):
        if self.check_dataset_model(model):
            fields_info = self.replace_type(model=model.__fields__.get('dataset').type_) 
        else:
            fields_info = self.replace_type(model=model)
        return fields_info

class CreateDropSql(BaseModel):

    @classmethod
    def get_create(cls, **customType):
        table_name= TableNameExporter().get_table_name(model=cls)
        if not customType:
            fields_info = TableFieldsInfoExporter().get_fields_info(model=cls)
        else:
            fields_info = customType
        type_part = ', '.join([f'{field} {type_}' for field, type_ in fields_info.items()])
        sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({type_part})'
        print(f'create sql : \n {sql}')
        return sql

    @classmethod
    def get_drop(cls):
        table_name = TableNameExporter().get_table_name(model=cls) 
        sql = f'DROP TABLE {table_name}'
        print(f'create sql : \n {sql}')
        return sql
    
class InsertSql:

    def get_values_part(self):
        data = self.dict().values()
        values_part_lst = [stringfy(value) for value in data]
        values_part = ', '.join(values_part_lst)
        return f'({values_part})'

    def get_values_parts(self):
        values_part_lst = [data.get_values_part() for data in self.dataset]
        values_part = ', '.join(values_part_lst)
        return values_part

    def get_insert(self):
        table_name = TableNameExporter().get_table_name(model = self.__class__)
        fields = list(TableFieldsInfoExporter().get_fields_info(model=self.__class__).keys())
        if TableIdentifier().check_dataset_model(model=self.__class__):
            values_part = self.get_values_parts()
        else:
            values_part = self.get_values_part()
        fields_part = ', '.join(fields)
        sql = f'INSERT INTO {table_name} ({fields_part}) VALUES{values_part}'
        print(f'insert sql : \n {sql}')
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

class Sql(CreateDropSql, InsertSql, DeleteSql, UpdateSql):
    pass
