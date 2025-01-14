# 一、mysql异步engine
# pip install asyncmy

from sqlalchemy.ext.asyncio import create_async_engine
async_mysql_engine = create_async_engine("mysql+asyncmy://root:root@localhost:3306/db_name",pool_recycle=3600)

# 二、postgresql异步engine
# pip install asyncpg

async_postgresql_engine = create_async_engine("postgresql+asyncpg://root:root@localhost:5432/db_name",echo=True,)

# 三、SQLserver异步engine
# pip install aioodbc

async_sqlsever_engine = create_async_engine(
    "mssql+aioodbc://root:root@localhost/db_name?"
    "driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
)

# 四、oracle异步engine
from sqlalchemy.ext.asyncio import create_async_engine
async_oracle_engine = create_async_engine("oracle+oracledb_async://scott:tiger@localhost/?service_name=XEPDB1")

async_oracle_engine1 = create_async_engine("oracle+oracledb://scott:tiger@localhost/?service_name=XEPDB1")

'''
aiosqlite：用于连接 SQLite
asyncmy、aiomysql：用于连接 MySQL
asyncpg、aiopg：用于连接 PostgreSQL
cx_Oracle_async：用于连接 Oracle
cx_Oracle_async：用于连接 Oracle
'''
