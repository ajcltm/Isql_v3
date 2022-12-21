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
