# Task Executor
An extension to `asyncio.TaskGroup` that adds a `submit` method like
`concurrent.futures.ThreadPoolExecutor`. The rate at which coroutines
`submit`ted this way are scheduled can be limited by either or both of the following:
- `max_concurrency`: The maximum number of `submit`ted coroutines that can be scheduled concurrently at a time. Similar effect as the `max_workers` parameter for
`concurrent.futures.ThreadPoolExecutor`.
- `rate_limit`: The maximium number of coroutines that can be scheduled per second.

Note that `create_task` can still be used. It behaves exactly as it does for `asyncio.TaskGroup`.
Tasks created that way are unaffected by and are completely independent from coroutines
submitted via `submit`, and have no impact on the rate at which `submit`ted coroutines are scheduled.
