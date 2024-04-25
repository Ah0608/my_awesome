import asyncio
from asyncio import sleep


class async_request:
    def __init__(self, max_concurrency=5):
        self.max_workers = max_concurrency
        self.queue = asyncio.Queue()

    async def fetch(self, ):
        while True:
            if self.queue.qsize() == 0:
                exit()
            item = await self.queue.get()
            function, line, args, kwargs = item
            await function(line, *args, **kwargs)

    async def request(self, ):
        workers = [self.fetch() for _ in range(self.max_workers)]
        print(len(workers))
        await asyncio.gather(*workers)

    async def add_task(self, function, data, *args, **kwargs):
        for line in data:
            await self.queue.put((function, line, args, kwargs))
        await self.request()


async def Hello(message, st):
    print(message, st)
    await sleep(2)


async def main():
    urls = ['你好{}'.format(i) for i in range(1, 10000)]
    client = async_request(max_concurrency=5)
    await client.add_task(Hello, urls, '哈哈')


if __name__ == '__main__':
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(asyncio.gather(main()))
    asyncio.run(main())
