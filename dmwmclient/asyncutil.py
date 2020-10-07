import asyncio


async def completed(coroutines, semaphore=None):
    """as_completed as an async generator

    Takes a list of coroutines, and executes them as tasks, yielding
    the results as they are completed.

    Parameters
    ----------
        semaphore : AbstractAsyncContextManager or int, optional
            A semaphore to limit concurrency
    """
    if semaphore is None:
        for coro in asyncio.as_completed(list(coroutines)):
            yield await coro
    else:
        if isinstance(semaphore, int):
            semaphore = asyncio.BoundedSemaphore(semaphore)

        async def throttle(coro):
            async with semaphore:
                return await coro

        for coro in asyncio.as_completed(map(throttle, coroutines)):
            yield await coro
