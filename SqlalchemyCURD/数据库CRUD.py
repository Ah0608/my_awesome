import traceback
from typing import Union

from sqlalchemy import MetaData, Table, text
from sqlalchemy.dialects.mysql import insert as my_ins
from sqlalchemy.dialects.postgresql import insert as pg_ins
from sqlalchemy.orm import sessionmaker


class DBOperate:
    def __init__(self, db_engine):
        self.engine = db_engine
        self.tables_cache = {}
        self.metadata = MetaData()
        self.metadata.reflect(bind=db_engine)
        self.session = sessionmaker(self.engine, expire_on_commit=False)

    def get_table(self, table_name: str) -> Table:
        if '.' in table_name:
            schema, table_name = table_name.split('.')
        else:
            schema = None
        cache_key = f"{schema}.{table_name}" if schema else table_name
        if cache_key not in self.tables_cache:
            table = Table(table_name, self.metadata, autoload_with=self.engine, schema=schema)
            self.tables_cache[cache_key] = table
        return self.tables_cache[cache_key]

    @staticmethod
    def execute_transaction(session, stmt, data=None):
        transaction = session.begin()
        try:
            session.execute(stmt, data)
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            traceback.print_exc()
            raise RuntimeError('sql执行, 已回滚!', e)

    def my_insert(self, table_name: Union[Table, str], item: Union[dict, list]):
        if isinstance(table_name, str):
            table = self.get_table(table_name)
        else:
            table = table_name
        with self.session() as sess:
            if isinstance(item, dict):
                insert_stmt = my_ins(table).values(**item)
                stmt = insert_stmt.on_duplicate_key_update(**item)
                self.execute_transaction(sess, stmt)
            elif isinstance(item, list):
                stmt = table.insert().values(item)
                self.execute_transaction(sess, stmt)

    def pg_insert(self, table_name: Union[Table, str], item: Union[dict, list], index_elements: list = None):
        if isinstance(table_name, str):
            table = self.get_table(table_name)
        else:
            table = table_name
        with self.session() as sess:
            if isinstance(item, dict):
                insert_stmt = pg_ins(table).values(**item)
                # stmt = insert_stmt.on_conflict_do_update(index_elements=index_elements, set_=item) # 更新
                stmt = insert_stmt.on_conflict_do_nothing(index_elements=index_elements, set_=item) # 什么都不做
            elif isinstance(item, list):
                stmt = table.insert().values(item)
            self.execute_transaction(sess, stmt)

    def insert_all(self, data_list: list):
        with self.session() as sess:
            for table_name, value_list in data_list:
                if isinstance(table_name, str):
                    table = self.get_table(table_name)
                else:
                    table = table_name
                self.execute_transaction(sess, table.insert().values(value_list))

    def query(self, sql, **params):
        with self.session() as sess:
            stmt = text(sql)
            result = sess.execute(stmt, params)
            return result.fetchall()

    def update(self, sql, **params):
        with self.session() as sess:
            stmt = text(sql)
            self.execute_transaction(sess, stmt, params)