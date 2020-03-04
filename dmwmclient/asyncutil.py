import asyncio


async def completed(futures):
    """as_completed as an async generator"""
    for future in asyncio.as_completed(list(futures)):
        yield await future
