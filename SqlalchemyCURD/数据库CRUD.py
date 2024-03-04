import traceback
from typing import Union

from sqlalchemy import MetaData, Table, text

class DBOperate:
    def __init__(self,db_engine):
        self.engine = db_engine

        self.metadata = MetaData()
        self.metadata.reflect(bind=db_engine)

    def get_table(self, table_name: str) -> Table:
        """获取table对象"""
        return Table(table_name, self.metadata, autoload_with=self.engine)

    def insert(self, table_name: Union[Table, str], item: Union[dict, list]):
        """
        :param table_name:
        :param item:
        用法: 将dict、list格式的数据插入到数据库表中
        mysql_engine,tables = get_tables()
        curd = DBOperate(mysql_engine)
        student_dict = {'s_no':'2024002','s_name':'lisa','s_gender':'female','s_age':'18'}
        grade_dict = [{'s_no':'2024002','course':'math','score':'98'},{'s_no':'2024002','course':'english','score':'95'}]
        curd.insert(tables.student_table,student_dict)
        curd.insert(tables.grade_table,grade_dict)
        """
        if isinstance(table_name, str):
            table = self.get_table(table_name)
        else:
            table = table_name
        with self.engine.connect() as connection:
            transaction = connection.begin()  # 开始sql事务
            try:
                if isinstance(item, dict):
                    connection.execute(table.insert().values(**item))
                elif isinstance(item, list):
                    connection.execute(table.insert(), item)
                transaction.commit()  # 数据写入执行成功，数据提交到数据库文件
            except BaseException as e:
                transaction.rollback()  # 数据写入失败或者sql执行失败，会回滚这个事务中所有执行的sql，数据库中就不会出现报错整段数据
                traceback.print_exc()
                raise RuntimeError('数据插入报错，数据已回滚！',e)

    def insert_all(self, data_list :list):
        '''
        :param data_list: 例子[(tables.student_table, [student_dict,])]
        用法: 通过创建数据库的get_tables()方法获取所有表的名称、数据库引擎
        mysql_engine,tables = get_tables()
        curd = DBOperate(mysql_engine)
        data_list = []
        student_dict = {'s_no':'2024002','s_name':'lisa','s_gender':'female','s_age':'18'}
        data_list.append((tables.student_table, [student_dict]))
        grade_dict = [{'s_no':'2024002','course':'math','score':'98'},{'s_no':'2024002','course':'english','score':'95'}]
        data_list.append((tables.grade_table, grade_dict))
        curd.insert_all(data_list)
        '''
        with self.engine.connect() as connection:
            transaction = connection.begin()  # 开始sql 事务
            try:
                for table_name, value_list in data_list:
                    if isinstance(table_name, str):
                        table = self.get_table(table_name)
                    else:
                        table = table_name
                    connection.execute(table.insert(), value_list)
                transaction.commit()
            except BaseException as e:
                transaction.rollback()
                traceback.print_exc()
                raise RuntimeError('执行出错请检查！',e)

    def query(self, sql, **params):
        """
        用法:
        mysql_engine = create_engine("mysql+pymysql://root:root@localhost:3306/demo2",pool_recycle=3600)
        curd = DBOperate(mysql_engine)
        data = curd.query('SELECT * FROM student_info where s_no = :value1',value1='2024001')
        for line in data:
            print(line)
        """
        with self.engine.connect() as connection:
            stmt = text(sql)
            # 执行查询操作
            result = connection.execute(stmt, params)
            return result.fetchall()

    def update(self, sql, **params):
        """
        用法:
        mysql_engine = create_engine("mysql+pymysql://root:root@localhost:3306/demo2",pool_recycle=3600)
        curd = DBOperate(mysql_engine)
        curd.update('update student_info set s_age=:age where s_no=:sno',age=19,sno='2024002')
        """
        with self.engine.connect() as connection:
            transaction = connection.begin()  # 开始sql 事务
            try:
                stmt = text(sql)
                # 执行查询操作
                connection.execute(stmt, params)
                transaction.commit()  # 数据写入执行成功，数据提交到数据库
            except BaseException as e:
                transaction.rollback()  # 数据写入失败或者sql执行失败，会回滚这个事务中所有执行的sql，数据库中就不会出现报错整段数据
                traceback.print_exc()
                raise RuntimeError('数据更新错误，数据已回滚！',e)