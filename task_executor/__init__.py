import asyncio
from time import perf_counter


class TaskExecutor(asyncio.TaskGroup):
    def __init__(self, max_concurrency: int | None = None, rate_limit: float | None = None):
        """
        A TaskGroup but with additional functionality to limit the rate at which coroutines are scheduled.
        Note that ``create_task`` continues to work exactly as it does for regular :class:`TaskGroup`s,
        i.e. ``create_task`` behavior is not affected by :class:`TaskExecutor` configuration
        and is independent of coroutines scheduled using ``submit``.

        :param max_concurrency: If provided, the maximum number of ``submit``ted coroutines
        that can run concurrently at a time. Similar to the ``max_workers`` parameter
        for ``ThreadPoolExecutor`` and ``ProcessPoolExecutor`` from ``concurrent.futures``.
        :param rate_limit: If provided the maximum rate at which coroutines can be scheduled
        in coroutines per second.
        """
        super().__init__()
        self._futures: set[asyncio.Future] = set()
        self.__queue: asyncio.Queue | None = None
        self.__scheduler_task: asyncio.Task | None = None

        # Keep references to created futures in case they need to be cancelled due to TaskGroup abort
        # If max_concurrency is provided, create a semaphore used to enforce it
        self.__semaphore = asyncio.BoundedSemaphore(max_concurrency) if max_concurrency else None

        # If rate_limit is provided, compute the minimum duration between coroutine executions
        if rate_limit:
            self.__min_interval = 1 / rate_limit
            self.__last_scheduled_time = perf_counter()
        else:
            self.__min_interval = self.__last_scheduled_time = None

    async def __aenter__(self):
        await super().__aenter__()
        # Create queue to store coroutines to be scheduled
        self.__queue = asyncio.Queue()
        # Create worker task to schedule submitted coroutines at the appropriate time
        self.__scheduler_task = self.create_task(self.__scheduler())
        return self

    async def __aexit__(self, et, exc, tb):
        # Cancel the scheduler so the TaskGroup can close
        # TODO: Should this be done only once all futures are done?
        self.__scheduler_task.cancel()
        await super().__aexit__(et, exc, tb)
        self.__scheduler_task = None
        self.__queue = None

    def _abort(self):
        super()._abort()
        # As part of the abort process, cancel all outstanding futures
        for future in self._futures:
            if not future.done():
                future.cancel()

    async def __scheduler(self):
        """
        A worker task that schedules ``submit``ted coroutines based on the provided configuration.
        Runs indefinitely until cancelled when the TaskExecutor is closed.
        """
        while True:
            # Get the next coroutine from the queue
            wrapped_coro, name, context = await self.__queue.get()

            # If max_concurrency was provided, wait for an open slot if necessary
            if self.__semaphore is not None:
                await self.__semaphore.acquire()

            # If rate_limit was provided, wait if necessary to ensure that rate is not exceeded
            if self.__min_interval is not None:
                current_time = perf_counter()
                timedelta = self.__last_scheduled_time + self.__min_interval - current_time
                if timedelta > 0:
                    await asyncio.sleep(timedelta)
                else:
                    timedelta = 0
                self.__last_scheduled_time = current_time + timedelta

            # Schedule execution
            self.create_task(wrapped_coro, name=name, context=context)

    async def __coro_wrapper(self, future: asyncio.Future, coro):
        """
        Run a coroutine and put the result in a future.

        :param future: A Future in which to put the result of a coroutine
        :param coro: The coroutine to run
        """
        # Run the coroutine
        try:
            if not future.cancelled():
                result = await coro
        # Handle coroutine result appropriately
        except Exception as err:
            if not future.cancelled():
                future.set_exception(err)
        else:
            if not future.cancelled():
                future.set_result(result)
        # If max_concurrency was provided, free up a slot for a new task regardless of coroutine result
        finally:
            if self.__semaphore is not None:
                self.__semaphore.release()

    def submit(self, coro, *, name=None, context=None) -> asyncio.Future:
        """
        Schedules a coroutine to be executed and returns a :class:`asyncio.Future`
        representing the execution of the coroutine.
        The signature matches that of :meth:`asyncio.create_task`
        and :meth:`asyncio.TaskGroup.create_task`

        :param coro: The coroutine for which to create a :class:`asyncio.Future`.
        :param name: If not None, it is set as the name of the task using :meth:`asyncio.Task.set_name`.
        :param context: If provided, a custom :class:`contextvars.Context` for the coro to run in.
        The current context copy is created when no context is provided.
        """
        future = self._loop.create_future()
        # Keep references to created futures in case they need to be cancelled due to TaskGroup abort
        self._futures.add(future)
        # Remove internal reference once future is complete
        future.add_done_callback(self._futures.discard)
        # Wrap coroutine to output its result to the future
        wrapped_coro = self.__coro_wrapper(future, coro)
        # Add coroutine to queue to await execution
        self.__queue.put_nowait((wrapped_coro, name, context))
        return future
