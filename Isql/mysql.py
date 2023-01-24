from Isql import utils
from pydantic import BaseModel

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
        table_name= utils.TableNameExporter().get_table_name(model=cls)
        fields_info = utils.TableFieldsInfoExporter().get_fields_info(model=cls)
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
        table_name= utils.TableNameExporter().get_table_name(model=cls)
        primaryKey = cls.handle_list(primaryKey)
        sql = f'ALTER TABLE {table_name} ADD PRIMARY KEY ({primaryKey})'
        print(f'primaryKey sql : \n {sql}')
        return sql
    
    @classmethod
    def get_drop_primaryKey(cls):
        table_name= utils.TableNameExporter().get_table_name(model=cls)
        sql = f'ALTER TABLE {table_name} DROP PRIMARY KEY'
        print(f'primaryKey sql : \n {sql}')
        return sql

    @classmethod
    def get_drop(cls):
        table_name = utils.TableNameExporter().get_table_name(model=cls) 
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
        table_name= utils.TableNameExporter().get_table_name(model=cls)
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
        table_name= utils.TableNameExporter().get_table_name(model=cls)
        index_name = cls.get_index_name(index)
        index = cls.handle_list(index)
        if unique:
            sql = f'ALTER TABLE {table_name} ADD UNIQUE INDEX IF NOT EXISTS {index_name} ({index})'
        else:
            sql = f'ALTER TABLE {table_name} ADD INDEX IF NOT EXISTS {index_name} ({index})'
        print(f'index sql : \n {sql}')
        return sql
        
    @classmethod
    def get_drop_index(cls, index):
        table_name= utils.TableNameExporter().get_table_name(model=cls)
        index_name = cls.get_index_name(index)
        sql = f'DROP INDEX {index_name} ON {table_name}'
        print(f'index sql : \n {sql}')
        return sql


class InsertSql:

    def get_insert(self):
        table_name = utils.TableNameExporter().get_table_name(model = self.__class__)
        fields = list(utils.TableFieldsInfoExporter().get_fields_info(model=self.__class__).keys())
        if utils.TableIdentifier().check_dataset_model(model=self.__class__):
            data = tuple(self.data[0].dict().values())
        else:
            data = tuple(self.dict().values()) 
        fields_part = ', '.join(fields)
        q_marks = '('+','.join(['%s' for i in range(0, len(data))])+')'
        sql = f'INSERT INTO {table_name} ({fields_part}) VALUES {q_marks}' 
        print(f'insert sql : \n {sql[:1000]}')
        return sql

class DeleteSql:

    @classmethod
    def get_delete(cls, **condition):
        table_name = utils.TableNameExporter().get_table_name(model=cls)
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
            ls = [f"{key}=?" for key in values.keys()]
            value_part = 'SET ' + ', '.join(ls)
            return value_part
        table_name = utils.TableNameExporter().get_table_name(model=cls)
        value_part = get_value_part(**values)
        sql = f"UPDATE {table_name} {value_part}"
        return cls.Where(sql) 

    class Where:
        def __init__(self, sql):
            self.sql = sql
        def where(self, **arg):
            ls = [f"{key}=?" for key in arg.keys()]
            where_part = 'WHERE ' + ' and '.join(ls)
            sql = f"{self.sql} {where_part}"
            print(f'update sql : \n {sql}')
            return sql


class Mysql(CreateDropSql, IndexSql, InsertSql, DeleteSql, UpdateSql):
    pass

