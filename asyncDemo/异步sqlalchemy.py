import traceback
from typing import Union

from sqlalchemy import text, Table, MetaData
from sqlalchemy.dialects.mysql import insert as my_ins
from sqlalchemy.dialects.postgresql import insert as pg_ins
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker


class AsyncDBOperate:
    def __init__(self, db_engine):
        self.engine = db_engine
        self.metadata = MetaData()
        self.async_session = sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)
        self.tables_cache = {}

    async def close_engine(self):
        await self.engine.dispose()

    async def get_table(self, table_name: str) -> Table:
        if '.' in table_name:
            schema, table_name = table_name.split('.')
        else:
            schema = None
        cache_key = f"{schema}.{table_name}" if schema else table_name
        if cache_key not in self.tables_cache:
            async with self.engine.connect() as conn:
                await conn.run_sync(self.metadata.reflect, schema=schema, only=[table_name])
                table = Table(table_name, self.metadata, autoload_with=self.engine, schema=schema)
                self.tables_cache[cache_key] = table
        return self.tables_cache[cache_key]

    @staticmethod
    async def execute_transaction(session, stmt, data=None):
        transaction = await session.begin()
        try:
            await session.execute(stmt, data)
            await transaction.commit()
        except Exception as e:
            await transaction.rollback()
            traceback.print_exc()
            raise RuntimeError('sql执行, 已回滚!', e)

    async def my_insert(self, table_name: Union[Table, str], item: Union[dict, list]):
        if isinstance(table_name, str):
            table = await self.get_table(table_name)
        else:
            table = table_name

        async with self.async_session() as session:
            if isinstance(item, dict):
                insert_stmt = my_ins(table).values(**item)
                stmt = insert_stmt.on_duplicate_key_update(**item)  # 主键或唯一索引重复则更新
            elif isinstance(item, list):
                stmt = table.insert().values(item)
            await self.execute_transaction(session, stmt)

    async def pg_insert(self, table_name: Union[Table, str], item: Union[dict, list], index_elements: list):
        if isinstance(table_name, str):
            table = await self.get_table(table_name)
        else:
            table = table_name

        async with self.async_session() as session:
            if isinstance(item, dict):
                insert_stmt = pg_ins(table).values(**item)
                stmt = insert_stmt.on_conflict_do_update(index_elements=index_elements, set_=item)  # 主键或唯一索引重复则更新
            elif isinstance(item, list):
                stmt = table.insert().values(item)
            await self.execute_transaction(session, stmt)

    async def insert_all(self, data_list: list):
        async with self.async_session() as session:
            for table_name, value_list in data_list:
                if isinstance(table_name, str):
                    table = await self.get_table(table_name)
                else:
                    table = table_name
                await self.execute_transaction(session, table.insert().values(value_list))

    async def query(self, sql, **params):
        async with self.async_session() as session:
            stmt = text(sql)
            result = await session.execute(stmt, params)
            return result.fetchall()

    async def update(self, sql, **params):
        async with self.async_session() as session:
            stmt = text(sql)
            await self.execute_transaction(session, stmt, params)