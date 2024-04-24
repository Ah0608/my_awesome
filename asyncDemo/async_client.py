import asyncio


class async_request:
    def __init__(self,max_concurrency=5):
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.queue = []

    async def fetch(self, function,args,kwargs):
        async with self.semaphore:
            await function(*args, **kwargs)

    async def request(self,):
        tasks = []
        for item in self.queue:
            function, args, kwargs = item
            task = asyncio.ensure_future(self.fetch(function,args,kwargs))
            tasks.append(task)
        await asyncio.gather(*tasks)

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(self.request()))

    def add_task(self, function, *args, **kwargs):
        if kwargs is None:
            kwargs = {}
        self.queue.append((function, args, kwargs))