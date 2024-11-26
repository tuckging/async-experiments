import asyncio
from functools import wraps
import time
from typing import Callable

START_TIME = time.monotonic()

COLOURS = [
   '\033[1;35;48m', # PURPLE
   '\033[1;36;48m', # CYAN
   '\033[1;37;48m', # BOLD
   '\033[1;34;48m', # BLUE
   '\033[1;32;48m', # GREEN
   '\033[1;33;48m', # YELLOW
   '\033[1;31;48m', # RED
   '\033[1;30;48m', # BLACK
#    '\033[4;37;48m', # UNDERLINE
#    '\033[1;37;0m', # END
]

def prints_entry_exits(indent=0):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            first_integer_argument = next(a for a in args if isinstance(a, int))
            colour_index = first_integer_argument % len(COLOURS)
            print(COLOURS[colour_index] + f"[{time.monotonic() - START_TIME:05.2f}s] {' '*indent}Invoking {func.__name__}({first_integer_argument})...")
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            print(COLOURS[colour_index] + f"[{time.monotonic() - START_TIME:05.2f}s] {' '*indent}{func.__name__}({first_integer_argument}) returned {result}")
            return result
        return wrapper
    return decorator

CRAWL_TIME = 0.5
PARSE_TIME = 0.01
DB_TIME = 0.1
# how many concurrent tasks to run on a single thread
# use this to limit memory usage
CONCURRENCY = 4
WORK_SIZE = 15

@prints_entry_exits(indent=4)
async def crawl(i: int):
    # simulate blocking call to prepare request
    time.sleep(CRAWL_TIME)
    # simulate async call to server
    await asyncio.sleep(CRAWL_TIME)
    return i

@prints_entry_exits(indent=8)
def parse(i: int):
    time.sleep(PARSE_TIME)
    return i

class BatchedDB:
    def __init__(self, batch_size):
        self.batch_size = batch_size
        self._queue = []
        self.queue_lock = asyncio.Lock()

    @prints_entry_exits(indent=12)
    async def add_to_db_queue(self, i):
        async with self.queue_lock:
            self._queue.append(i)
            if len(self._queue) >= self.batch_size:
                print("DB queue is full!")
                await self.flush(bypass_lock=True)

    async def flush(self, bypass_lock=False):
        print(f"Flushing {len(self._queue)} items to DB")
        if bypass_lock:
            await asyncio.sleep(DB_TIME)
            self._queue.clear()
            return
        else:
            async with self.queue_lock:
                await asyncio.sleep(DB_TIME)
                self._queue.clear()

# do not share this across different threads
thread_level_batched_db = BatchedDB(4)
async def pipeline(i: int):
    await crawl(i)
    await parse(i)
    return await thread_level_batched_db.add_to_db_queue(i)

async def gather_with_concurrency(n, *coros):
    semaphore = asyncio.Semaphore(n)

    async def sem_coro(coro):
        async with semaphore:
            return await coro

    return await asyncio.gather(*(sem_coro(c) for c in coros))


async def main():
    work = range(WORK_SIZE)
    results = await gather_with_concurrency(CONCURRENCY, *(pipeline(i) for i in work))
    # always remember to flush ;)
    await thread_level_batched_db.flush()
    print(results)


if __name__ == "__main__":
    asyncio.run(main())
