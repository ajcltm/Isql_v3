import unittest

from Isql import querySql

class Test_select_from(unittest.TestCase):

    @unittest.skip("")
    def test_without_subquery(self):
        table = 'table'
        sql = querySql.QuerySql().set_table(tableName=table).select().from_(table).export_sql()
        print('='*200, sql, '='*150, sep='\n')

    @unittest.skip("")
    def test_specific_fields_without_subquery(self):
        table = 'table'
        sql = querySql.QuerySql().set_table(tableName=table).select('name', 'age').from_(table).export_sql()
        print('='*200, sql, '='*150, sep='\n')

    @unittest.skip("")
    def test_with_more_than_one_table(self):
        table1 = 'table1'
        table2 = 'table2'
        sql = querySql.QuerySql().set_table(table1).set_table(table2).select('table1.name', 'table2.age').from_(table1, table2).export_sql()
        print('='*200, sql, '='*150, sep='\n')

    @unittest.skip("")
    def test_with_subquery(self):
        table1 = 'TestTable_1'
        subquery = querySql.QuerySql().set_table(tableName=table1).select('name', 'age').from_(table1)
        sql = querySql.QuerySql().set_table(table1).set_table(tableName='sq_1', subQuery=subquery).select('table1.age','sq_1.name').from_('sq_1').export_sql()
        print('='*200, sql, '='*150, sep='\n')

    @unittest.skip("")
    def test_specific_fields_with_subquery(self):
        table1 = 'table1'
        table2 = 'table2'
        subquery = querySql.QuerySql().set_table(tableName=table1).select('name', 'age').from_(table1)
        sql = querySql.QuerySql().set_table(tableName=table2).set_table(tableName='sq_1', subQuery=subquery).select('table2.name', 'sq_1.age').from_(table2,'sq_1').export_sql()
        print('='*200, sql, '='*150, sep='\n')

    @unittest.skip("")
    def test_specific_fields_with_two_subquery(self):
        table1 = 'table1'
        table2 = 'table2'
        table3 = 'table3'
        subquery = querySql.QuerySql().set_table(tableName=table1).select('name', 'age').from_(table1)
        subquery2 = querySql.QuerySql().set_table(tableName=table2).set_table(tableName='sq_1', subQuery=subquery).select('table2.name', 'sq_1.age').from_(table2,'sq_1')
        sql = querySql.QuerySql().set_table(tableName=table3).set_table(tableName='sq_2', subQuery=subquery2).select('table3.name', 'sq_2.age').from_(table3,'sq_2').export_sql()
        print('='*200, sql, '='*150, sep='\n')

    @unittest.skip("")
    def test_where(self):
        table = 'table1'
        sql = querySql.QuerySql().set_table(tableName=table).select().from_(table).where(table1_name = '=kim', age = '>10', first_name='in (lee, park)', date = '< 2022-01-01').export_sql()
        print('='*200, sql, '='*150, sep='\n')

    @unittest.skip("")
    def test_where_subquery(self):
        tableName_1 = 'table1'
        tableName_2 = 'table2'
        subquery = querySql.QuerySql().set_table(tableName=tableName_1).select('name', 'age').from_(tableName_1)
        sql = querySql.QuerySql().set_table(tableName=tableName_2).set_table(tableName='sq_1', subQuery=subquery).select('table2.name', 'sq_1.p_age').from_(tableName_2,'sq_1').where(sq_1_age='>10').export_sql()
        print('='*200, sql, '='*150, sep='\n')

    @unittest.skip("")
    def test_where_with_two_subquery(self):
        table1 = 'table1'
        table2 = 'table2'
        table3 = 'table3'
        subquery = querySql.QuerySql().set_table(tableName=table1).select('name', 'age').from_(table1)
        subquery2 = querySql.QuerySql().set_table(tableName=table2).set_table(tableName='sq_1', subQuery=subquery).select('table2.name', 'sq_1.age').from_(table2,'sq_1').where(sq_1_name = '=kim')
        sql = querySql.QuerySql().set_table(tableName=table3).set_table(tableName='sq_2', subQuery=subquery2).select('table3.name', 'sq_2.p_age').from_(table3,'sq_2').where(table3_age='>10').export_sql()
        print('='*200, sql, '='*150, sep='\n')

    @unittest.skip("")
    def test_where_subquery_scalar(self):
        tableName_1 = 'table1'
        tableName_2 = 'table2'
        subquery = querySql.QuerySql().set_table(tableName=tableName_1).select('name', 'age').from_(tableName_1)
        sql = querySql.QuerySql().set_table(tableName=tableName_2).set_table(tableName='sq_1', subQuery=subquery).select('table2.name', 'sq_1.p_age').from_(tableName_2,'sq_1').where(sq_1_age='>sq_1').export_sql()
        print('='*200, sql, '='*150, sep='\n')

    @unittest.skip("")
    def test_where_many_scalar_query(self):
        table1 = 'table1'
        table2 = 'table2'
        table3 = 'table3'
        subquery = querySql.QuerySql().set_table(tableName=table1).select('name', 'age').from_(table1).where(table1_name='=kim')
        subquery2 = querySql.QuerySql().set_table(tableName=table2).set_table(tableName='sq_1', subQuery=subquery).select('table2.name', 'sq_1.age').from_(table2,'sq_1').where(name = '=sq_1')
        sql = querySql.QuerySql().set_table(tableName=table3).set_table(tableName='sq_2', subQuery=subquery2).select('table3.name', 'sq_2.p_age').from_(table3,'sq_2').where(table3_age='>sq_2').export_sql()
        print('='*200, sql, '='*150, sep='\n')

    @unittest.skip("")
    def test_join(self):
        table1 = 'table1'
        table2 = 'table2'
        sql = querySql.QuerySql().set_table(table1).select('table1.name', 'table2.age').from_(table1).join(table2, how='left').on(table1_age='=table2.age').export_sql()
        print(sql)

    @unittest.skip("")
    def test_join_with_subquery(self):
        table1 = 'table1'
        table2 = 'table2'
        table3 = 'table3'
        subquery = querySql.QuerySql().set_table(table1).select('table1.name', 'table2.age').from_(table1).join(table2, how='left').on(table1_age='=table2.age')
        sql = querySql.QuerySql().set_table(tableName='sq', subQuery=subquery).set_table(table3).select('table3.id').from_(table3).where(table3_age='>sq').export_sql()
        print('='*200, sql, '='*150, sep='\n')

    @unittest.skip("")
    def test_groupBy(self):
        table1 = 'table1'
        sql = querySql.QuerySql().from_(table1).group_by('table1.name','table1.age').sum('table1.age').export_sql()
        print('='*200, sql, '='*150, sep='\n')

        sql = querySql.QuerySql().from_(table1).group_by('table1.name','table1.age').min('table1.age').export_sql()
        print('='*200, sql, '='*150, sep='\n')
        
        sql = querySql.QuerySql().from_(table1).group_by('table1.name','table1.age').max('table1.age').export_sql()
        print('='*200, sql, '='*150, sep='\n')
        
        sql = querySql.QuerySql().from_(table1).group_by('table1.name','table1.age').avg('table1.age').export_sql()
        print('='*200, sql, '='*150, sep='\n')
        
        sql = querySql.QuerySql().from_(table1).group_by('table1.name','table1.age').variance('table1.age').export_sql()
        print('='*200, sql, '='*150, sep='\n')
        
        sql = querySql.QuerySql().from_(table1).group_by('table1.name','table1.age').stddev('table1.age').export_sql()
        print('='*200, sql, '='*150, sep='\n')
        
        sql = querySql.QuerySql().from_(table1).group_by('table1.name','table1.age').count('table1.age').export_sql()
        print('='*200, sql, '='*150, sep='\n')
        
    @unittest.skip("")
    def test_join_with_subquery(self):
        table1 = 'table1'
        table2 = 'table2'
        table3 = 'table3'
        subquery = querySql.QuerySql().set_table(table1).select('table1.name', 'table2.age').from_(table1).join(table2, how='left').on(table1_age='=table2.age')
        sql = querySql.QuerySql().set_table(tableName='sq', subQuery=subquery).set_table(table3).select('table3.id').from_('sq').where(table3_age='>10').group_by('sq.name', 'sq.age').max('sq.age').export_sql()
        print('='*200, sql, '='*150, sep='\n')

    @unittest.skip("")
    def test_orderBy(self):
        table1 = 'table1'
        sql = querySql.QuerySql().from_(table1).group_by('table1.name','table1.age').sum('table1.age').order_by('table.age asc', 'table.name desc').export_sql()
        print('='*200, sql, '='*150, sep='\n')

    def test_orderBy_with_subQuery(self):
        table1 = 'table1'
        table2 = 'table2'
        table3 = 'table3'
        subquery = querySql.QuerySql().set_table(table1).select('table1.name', 'table2.age').from_(table1).join(table2, how='left').on(table1_age='=table2.age')
        sql = querySql.QuerySql().set_table(tableName='sq', subQuery=subquery).set_table(table3).select('table3.id').from_('sq').where(table3_age='>10').group_by('sq.name', 'sq.age').max('sq.age').order_by('sq.name', 'sq.age').export_sql()
        print('='*200, sql, '='*150, sep='\n')

if __name__ == '__main__':
    unittest.main()