# 1、创建MySQL引擎：
from sqlalchemy import create_engine

mysql_engine = create_engine("mysql+pymysql://root:root@localhost:3306/db_name")

# 2、创建SQL serve引擎：
sql_server_engine = create_engine('mssql+pymssql://root:root@localhost/db_name')

# 3、创建PostgreSQL引擎：
postgresql_engine = create_engine("postgresql+psycopg2://root:root@localhost/db_name")

# 4、创建Oracle引擎：
oracle_engine = create_engine("oracle://scott:tiger@127.0.0.1:1521/sidname")
oracle_engine1 = create_engine("oracle+cx_oracle://scott:tiger@tnsname")

# 5、创建sqlite引擎：
sqlite_engine = create_engine("sqlite:///foo.db")