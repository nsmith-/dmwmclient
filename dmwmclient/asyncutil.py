import asyncio
import contextlib


def _make_throttle(semaphore):
    if isinstance(semaphore, int):
        semaphore = asyncio.BoundedSemaphore(semaphore)
    elif not isinstance(semaphore, contextlib.AbstractAsyncContextManager):
        raise ValueError(f"Unrecognized semaphore type: {type(semaphore)}")

    async def throttle(coro):
        async with semaphore:
            return await coro

    return throttle


async def gather(coroutines, semaphore):
    """asyncio.gather with throttle

    Parameters
    ----------
        semaphore : AbstractAsyncContextManager or int, optional
            A semaphore to limit concurrency
    """
    return await asyncio.gather(*map(_make_throttle(semaphore), coroutines))


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
        for coro in asyncio.as_completed(coroutines):
            yield await coro
    else:
        for coro in asyncio.as_completed(map(_make_throttle(semaphore), coroutines)):
            yield await coro
