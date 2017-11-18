"""waitgroup module provides  asynchronous read/write lock and wait groups classes
"""

import asyncio
import typing


class WaitGroup:
    """WaitGroup simple implementation with asyncio.

    A WaitGroup waits for a collection of coroutines to finish.
    The main coroutine calls `add` to set the number of coroutines to wait for.
    Then each of the coroutine runs and calls `done` when finished.
    At the same time, `wait` can be used to block until all coroutines have finished.
    """

    def __init__(self):

        self._wait_event = asyncio.Event()
        self._wait_event.set()

        self._counter = 0

    def add(self, delta: int = 1):
        """Add adds delta, which may be negative, to the WaitGroup counter.
        If the counter becomes zero, all coroutines blocked on Wait are released.
        If the counter goes negative, add raises a RuntimeError.

        Note that calls with a positive delta that occur when the counter is zero must happen
        before a wait. Calls with a negative delta, or calls with a positive delta that start
        when the counter is greater than zero, may happen at any time.
        Typically this means the calls to `add` should execute before the statement creating the
        coroutine or other event to be waited for.

        Args:
            delta (int, optional): The number of coroutines to add or substract from the waitgroup.

        Raises:
            RuntimeError: If WaitGroup counter value goes below 0
        """

        self._counter += delta
        if self._counter < 0:
            raise RuntimeError(
                "WaitGroup counter can't be negative ({})".format(self._counter))
        if self._counter == 0 and not self._wait_event.is_set():
            self._wait_event.set()

    async def wait(self):
        """Wait blocks until the WaitGroup counter is zero.
        """

        if not self._wait_event.is_set():
            await self._wait_event.wait()

    def done(self):
        """Done decrements the WaitGroup counter by one.
        """

        self.add(-1)

    def __enter__(self):
        """Context manager entry function, calls add with a delta value of one
        and returns the WaitGroup
        """

        self.add(1)
        return self

    def __exit__(self, *args, **kwargs):
        """Context manager exit function, calls done on the WaitGroup
        """
        self.done()


class CancellableWaitGroup(WaitGroup):
    """A waitgroup that can cancel all tasks added to it, calling add when the task is added, and
    done when the tasks has completed.

    The CancellableWaitGroup keeps track of running tasks added to the group and provides two
    additional functions compared to the simplier WaitGroup class.
    The `run` method wraps coroutines in a task and add them to the wait group.
    Each tasks has a callback set to decrement the WaitGroup counter as they finish.
    """

    def __init__(self):
        super(CancellableWaitGroup, self).__init__()

        self._tasks = set()

    def add_async(self, coro_or_future: typing.Coroutine or asyncio.Future) -> asyncio.Task:
        """calls add, schedule the execution of a coroutine object: wrap it in a future, calls done
        when the future is done.
        Return a Task object.

        Args:
            coro_or_future: a coroutine or :class: asyncio.Future object: wrapped it in a future

        Raises:
            * RuntimeError: If the same task is added multiple time before it completes
            * TypeError: If coro_or_future is neithe a coroutine nor a future

        Returns:
            The wrapped coroutine
        """

        def _cb_internal(_, task):
            """Calls the `done` method on WaitGroup, removes from set of cancellable tasks
            """

            self.done()
            self._tasks.remove(task)

        task = None

        if isinstance(coro_or_future, asyncio.Future):
            if coro_or_future in self._tasks:
                raise RuntimeError(
                    "task {} has already been added to this waitgroup", coro_or_future)
            task = coro_or_future
        elif asyncio.iscoroutine(coro_or_future):
            task = asyncio.ensure_future(coro_or_future)
            task.add_done_callback(_cb_internal)
        else:
            raise TypeError("coro_or_future")

        self.add()
        self._tasks.add(task)

        return task

    def cancel_all(self):
        """Cancel all cancels all managed tasks
        """

        for task in self._tasks:
            task.cancel()
