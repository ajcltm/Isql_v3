from abc import ABC, abstractmethod

def replace_alias_for_field(field, table):
    splited = field.split('.')
    if len(splited)>1:
        if splited[0] in table:
            alias = table[splited[0]]['alias']
            if alias:
                return f'{alias}.{splited[1]}'
            else:
                return field
        else:
            return field
    return field

def replace_alias_for_variable(variable, table):
    for tableName in table.keys():
        splited = variable.split(tableName)
        if len(splited)>1:
            alias = table[tableName]['alias']
            if alias:
                no_under_bar = splited[-1].replace('_', '') 
                return f'{alias}.{no_under_bar}'
            else:
                no_under_bar = splited[-1].replace('_', '') 
                return f'{tableName}.{no_under_bar}'
    return variable

def handle_colon(value, table):
    if value in table:
        value = table[value]['subQuery'].export_sql()
        return f'({value})'
    if value.isdigit():
        return int(value)
    if value[0] =='(':
        value = value.strip()
        value = value[1:-1]
        splited = value.split(',')
        colon_splited = [f'"{v.strip()}"' for v in splited]
        colon_content = ', '.join(colon_splited)
        return f'({colon_content})'
    return f'"{value}"'

def divide_symbols_and_values(condition, table, colon=True):
    symbols = ['<=', '>=', '>', '<','=', 'in ']
    lst = []
    for field, con in condition.items():
        for symbol in symbols:
            splited = con.split(symbol)
            if len(splited) > 1:
                value = splited[-1].strip() 
                if colon:
                    value = handle_colon(value=value, table=table)
                symbol = symbol.strip()
                expression = f'{field} {symbol} {value}' 
                lst.append(expression)
                break
    con_str = ' and '.join(lst)
    return con_str

class Clause(ABC):

    def update_table(self, table):
        self.table = table
    
    def set_aliasState(self, aliasState):
        self.aliasState = aliasState
    
    @abstractmethod
    def export_sql(self, aliasState):
        pass

class Select(Clause):

    def __init__(self, fields):
        self.fields = fields
    
    def export_sql(self, aliasState):
        self.set_aliasState(aliasState=aliasState)
        if self.fields:
            adjusted_fields = [replace_alias_for_field(field, self.table) for field in self.fields]
            select_part = ', '.join(adjusted_fields)
        else:
            select_part = '*'
        return f'SELECT {select_part}'

class From(Clause):

    def __init__(self, tableName):
        self.tableName_list = tableName
    
    def check_inlineView(self, tableName):
        if not tableName in self.table:
            return False
        if self.table[tableName]['subQuery'] :
            return True
        return False

    def export_sql(self, aliasState):
        self.set_aliasState(aliasState=aliasState)
        table_list = []
        for tableName in self.tableName_list:
            if self.check_inlineView(tableName=tableName):
                inleview_clause = self.table[tableName]['subQuery'].export_sql(aliasState=aliasState)
                self.table[tableName]['alias'] = self.aliasState.get_alias()
                alias = self.table[tableName]['alias']
                table_list.append(f'({inleview_clause}) {alias}')
            else:
                if len(self.tableName_list)>1:
                    if tableName in self.table:
                        self.table[tableName]['alias'] = self.aliasState.get_alias()
                        alias = self.table[tableName]['alias']
                        table_list.append(f'{tableName} {alias}')
                    else:
                        table_list.append(f'{tableName}')
                else:
                    table_list.append(tableName)
        from_part = ', '.join(table_list)
        return f'FROM {from_part}'

class Join(Clause):

    def __init__(self, tableName, how):
        self.tableName=tableName
        self.how=how

    def check_inlineView(self, tableName):
        if self.table[tableName]['subQuery'] :
            return True
        return False

    def export_sql(self, aliasState):
        if not self.tableName in self.table:
            return f'{self.how} join {self.tableName}'

        self.set_aliasState(aliasState=aliasState)
        if self.check_inlineView(self.tableName):
            inleview_clause = self.table[self.tableName]['subQuery'].export_sql(aliasState=aliasState)
            self.table[self.tableName]['alias'] = self.aliasstate.get_alias()
            alias = self.table[self.tableName]['alias']
            return f'{self.how}join ({inleview_clause}) {alias}'
        else:
            if self.tableName in self.table:
                alias = self.table[self.tableName]['alias']
                return f'{self.how} join {self.tableName} {alias}'
            else:
                return f'{self.how} join {self.tableName}'
        
class On(Clause):

    def __init__(self, condition):
        self.condition = condition
    
    def export_sql(self, aliasState):
        self.set_aliasState(aliasState=aliasState)
        adjusted_keys = [replace_alias_for_variable(variable, self.table) for variable in self.condition.keys()]
        adjusted_condition = {adjusted_keys[i] : con for i, con in enumerate(self.condition.values())}
        where_part = divide_symbols_and_values(adjusted_condition, self.table, colon=False)
        return f'On {where_part}'

class Where(Clause):

    def __init__(self, condition):
        self.condition = condition
    
    def export_sql(self, aliasState):
        self.set_aliasState(aliasState=aliasState)
        adjusted_keys = [replace_alias_for_variable(variable, self.table) for variable in self.condition.keys()]
        adjusted_condition = {adjusted_keys[i] : con for i, con in enumerate(self.condition.values())}
        where_part = divide_symbols_and_values(adjusted_condition, self.table)
        return f'WHERE {where_part}'

class GroupBy(Clause):

    def __init__(self, fields):
        self.fields = fields
    
    def export_sql(self, aliasState):
        self.set_aliasState(aliasState=aliasState)
        adjusted_fields = [replace_alias_for_field(field, self.table) for field in self.fields]
        groupBy_part = ', '.join(adjusted_fields)
        return f'GROUP BY {groupBy_part}'

class Calculation(Clause):

    def __init__(self, field, kind):
        self.field = field
        self.kind = kind
        self.sql_fuction = {'sum': 'SUM', 'min':'MIN', 'max':'MAX', 'avg':'AVG', 'count':'COUNT' ,'variance':'VARIANCE', 'stddev':'STDDEV'}
    
    def export_sql(self, aliasState):
        self.set_aliasState(aliasState=aliasState)
        adjusted_field = replace_alias_for_field(self.field, self.table)
        sql_function = self.sql_fuction[self.kind]
        return f'{sql_function}({adjusted_field})'

class OrderBy(Clause):

    def __init__(self, fields):
        self.fields = fields
    
    def export_sql(self, aliasState):
        self.set_aliasState(aliasState=aliasState)
        adjusted_fields = [replace_alias_for_field(field, self.table) for field in self.fields]
        groupBy_part = ', '.join(adjusted_fields)
        return f'ORDER BY {groupBy_part}'

class Alias:

    def __init__(self) :
        self.alias_candidate = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k']
        self.alias_num = 0

    def get_alias(self):
        alias = self.alias_candidate[self.alias_num]
        self.alias_num += 1
        return alias

class QuerySql:

    def __init__(self):
        self.table = dict()
        self.subQuery = dict()

        self.select_part = None
        self.from_part = None
        self.join_part = None
        self.on_part = None
        self.where_part = None
        self.groupBy_part = None
        self.calculation_part = []
        self.orderBy_part = None
        self.alias_state = None
    
    def set_table(self, tableName, subQuery=None):
        self.table[tableName] = {'subQuery':subQuery, 'alias': None}
        if self.select_part:
            self.select_part.update_table(table=self.table)
        if self.from_part:
            self.from_part.update_table(table=self.table)
        if self.join_part:
            self.join_part.update_table(table=self.table)
        if self.where_part:
            self.where_part.update_table(table=self.table)
        return self
    
    def select(self, *fields):
        self.select_part = Select(fields=fields)
        self.select_part.update_table(table=self.table)
        return self
    
    def from_(self, *tableName):
        self.from_part = From(tableName=tableName)
        self.from_part.update_table(table=self.table)
        return self

    def join(self, tableName, how='left'):
        self.join_part = Join(tableName=tableName, how=how)
        self.join_part.update_table(table=self.table)
        return self
    
    def on(self, **condition):
        self.on_part = On(condition=condition)
        self.on_part.update_table(table=self.table)
        return self

    def where(self, **condition):
        self.where_part = Where(condition=condition)
        self.where_part.update_table(table=self.table)
        return self

    def group_by(self, *fields):
        self.groupBy_part = GroupBy(fields)
        self.groupBy_part.update_table(table=self.table)
        return self
    
    def sum(self, fields):
        self.calculation_part.append(Calculation(fields, kind='sum'))
        for part in self.calculation_part:
            part.update_table(table=self.table)
        return self

    def min(self, fields):
        self.calculation_part.append(Calculation(fields, kind='min'))
        for part in self.calculation_part:
            part.update_table(table=self.table)
        return self

    def max(self, fields):
        self.calculation_part.append(Calculation(fields, kind='max'))
        for part in self.calculation_part:
            part.update_table(table=self.table)
        return self

    def avg(self, fields):
        self.calculation_part.append(Calculation(fields, kind='avg'))
        for part in self.calculation_part:
            part.update_table(table=self.table)
        return self

    def variance(self, fields):
        self.calculation_part.append(Calculation(fields, kind='variance'))
        for part in self.calculation_part:
            part.update_table(table=self.table)
        return self
    
    def stddev(self, fields):
        self.calculation_part.append(Calculation(fields, kind='stddev'))
        for part in self.calculation_part:
            part.update_table(table=self.table)
        return self
    
    def count(self, fields):
        self.calculation_part.append(Calculation(fields, kind='count'))
        for part in self.calculation_part:
            part.update_table(table=self.table)
        return self

    def order_by(self, *fields):
        self.orderBy_part = OrderBy(fields)
        self.orderBy_part.update_table(table=self.table)
        return self
    
    def export_sql(self, aliasState=None):
        if not aliasState:
            self.alias_state = Alias()
            aliasState = self.alias_state
        loop_order = {'from':self.from_part, 'join':self.join_part, 'on':self.on_part, 'groupBy':self.groupBy_part, 'select':self.select_part, 'calculation':self.calculation_part, 'where':self.where_part, 'orderBy': self.orderBy_part}
        clause_order = {'select':'', 'from':'', 'join':'', 'on':'', 'where':'', 'groupBy':'', 'orderBy':''}
        for clause, part in loop_order.items():
            if part:
                if clause=='calculation':
                    lst = [i.export_sql(aliasState) for i in part] 
                    if self.select_part:
                        lst = [clause_order['select']] + lst
                        clause_order['select'] = ', '.join(lst)
                    else:
                        clause_order['select'] = 'SELECT ' + ', '.join(lst)
                else:
                    clause_order[clause] = part.export_sql(aliasState)
        part_list = list(clause_order.values()) 
        return ' '.join(part_list)
