from sqlalchemy import Table, Integer, Column, MetaData, NVARCHAR, BigInteger
from sqlalchemy.dialects.mssql import NTEXT
from sqlalchemy import create_engine

class CreateTable:
    def __init__(self, db_name, student_table_name,grade_table_name):
        self.metadata = MetaData(schema=db_name)
        self.student_table = Table(
            student_table_name, self.metadata,
            Column("ID", Integer(),primary_key=True),
            Column("s_no", NVARCHAR(100),nullable=False),
            Column("s_name", NVARCHAR(100),nullable=False),
            Column("s_gender", NVARCHAR(50),nullable=False),
            Column("s_age", NVARCHAR(100),nullable=False),)

        self.grade_table = Table(
            grade_table_name, self.metadata,
            Column("ID", Integer(), primary_key=True),
            Column("s_no", NVARCHAR(100), nullable=False),
            Column("course", NVARCHAR(100), nullable=False),
            Column("score", NVARCHAR(50), nullable=False),)

    def create(self, engine):
        self.metadata.create_all(engine)



def get_tables():
    mysql_engine = create_engine("mysql+pymysql://root:root@localhost:3306/demo2",pool_recycle=3600)
    # 创建数据库表
    tables = CreateTable('demo2','student_info','grade_info')
    tables.create(mysql_engine)
    print('create tables is successfully!')
    return mysql_engine,tables # 返回数据库引擎、创建表的所有信息

if __name__ == '__main__':
    get_tables()