import asyncio
from time import perf_counter

import pytest

from task_executor import TaskExecutor


@pytest.mark.asyncio
async def test_max_concurrency():
    sentinel = object()
    outputs = []

    start_time = perf_counter()
    async with TaskExecutor(max_concurrency=100) as executor:
        futures = [executor.submit(asyncio.sleep(0.5, sentinel)) for i in range(400)]
        for future in asyncio.as_completed(futures):
            output = await future
            outputs.append(output)
    duration = perf_counter() - start_time
    print(duration)

    assert duration > 2
    assert duration < 2.1
    assert all(i is sentinel for i in outputs)


@pytest.mark.asyncio
async def test_rate_limit():
    sentinel = object()
    outputs = []

    start_time = perf_counter()
    async with TaskExecutor(rate_limit=20) as executor:
        futures = [executor.submit(asyncio.sleep(0, sentinel)) for i in range(40)]
        for future in asyncio.as_completed(futures):
            output = await future
            outputs.append(output)
    duration = perf_counter() - start_time
    print(duration)

    assert duration > 2
    assert duration < 2.05
    assert all(i is sentinel for i in outputs)
