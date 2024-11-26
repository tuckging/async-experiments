import asyncio
from functools import wraps
import time

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
   '\033[4;37;48m', # UNDERLINE
   '\033[1;37;0m', # END
]

def prints_entry_exits(indent=""):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            print(COLOURS[args[0]] + f"[{time.monotonic() - START_TIME:.1f}] {indent}Invoking {func.__name__}({args[0]}) ...")
            result = await func(*args, **kwargs)
            print(COLOURS[args[0]] + f"[{time.monotonic() - START_TIME:.1f}] {indent}{func.__name__}({args[0]}) returned: {result}")
            return result
        return wrapper
    return decorator

CRAWL_TIME = 0.5
PARSE_TIME = 0.01
DB_TIME = 0.1

@prints_entry_exits(indent="  ")
async def crawl(i: int):
    # simulate blocking call
    time.sleep(CRAWL_TIME)
    # simulate async call
    await asyncio.sleep(CRAWL_TIME)
    return i

@prints_entry_exits(indent="  ")
async def parse(i: int):
    await asyncio.sleep(PARSE_TIME)
    return i

@prints_entry_exits(indent="  ")
async def db(i: int):
    await asyncio.sleep(DB_TIME)
    return i

@prints_entry_exits()
async def pipeline(i: int):
    await crawl(i)
    await parse(i)
    return await db(i)

async def gather_with_concurrency(n, *coros):
    semaphore = asyncio.Semaphore(n)

    async def sem_coro(coro):
        async with semaphore:
            return await coro

    return await asyncio.gather(*(sem_coro(c) for c in coros))


async def main():
    work = range(9)
    results = await gather_with_concurrency(2, *(pipeline(i) for i in work))
    print(results)


if __name__ == "__main__":
    asyncio.run(main())
