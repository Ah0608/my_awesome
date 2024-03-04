import traceback
from typing import Union

from sqlalchemy import text, Table, MetaData
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

class AsyncDBOperate:
    def __init__(self, db_engine):
        self.engine = db_engine
        self.metadata = MetaData()
        self.async_session = sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)

    async def colse_engine(self):
        await self.engine.dispose()

    async def get_table(self, table_name: str) -> Table:
        async with self.engine.connect() as conn:
            await conn.run_sync(self.metadata.reflect, only=[table_name])
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            return table

    async def insert(self, table_name: Union[Table, str], item: Union[dict, list]):
        if isinstance(table_name, str):
            table = await self.get_table(table_name)
        else:
            table = table_name
        async with self.async_session() as session:
            transaction = await session.begin()  # 开始sql事务
            try:
                if isinstance(item, dict):
                    await session.execute(table.insert().values(**item))
                elif isinstance(item, list):
                    await session.execute(table.insert(), item)
                await transaction.commit()  # 数据写入执行成功，数据提交到数据库文件
            except BaseException as e:
                await transaction.rollback()  # 数据写入失败或者sql执行失败，会回滚这个事务中所有执行的sql，数据库中就不会出现报错整段数据
                await traceback.print_exc()
                raise RuntimeError('数据插入报错，数据已回滚！',e)

    async def insert_all(self, data_list :list):
        async with self.async_session() as session:
            transaction = await session.begin()  # 开始sql 事务
            try:
                for table_name, value_list in data_list:
                    if isinstance(table_name, str):
                        table = await self.get_table(table_name)
                    else:
                        table = table_name
                    await session.execute(table.insert(), value_list)
                await transaction.commit()
            except BaseException as e:
                await transaction.rollback()
                await traceback.print_exc()
                raise RuntimeError('执行出错请检查！',e)

    async def query(self, sql, **params):
        async with self.async_session() as session:
            stmt = text(sql)
             # 执行查询操作
            result = await session.execute(stmt, params)
            return result.fetchall()

    async def update(self, sql, **params):
        async with self.async_session() as session:
            transaction = await session.begin()  # 开始sql 事务
            try:
                stmt = text(sql)
                # 执行查询操作
                await session.execute(stmt, params)
                await transaction.commit()  # 数据写入执行成功，数据提交到数据库
            except BaseException as e:
                await transaction.rollback()  # 数据写入失败或者sql执行失败，会回滚这个事务中所有执行的sql，数据库中就不会出现报错整段数据
                await traceback.print_exc()
                raise RuntimeError('出现错误，数据已回滚！',e)