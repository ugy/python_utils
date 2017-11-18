"""async.lock module provides a asynchronous RWLock working on top of asyncio Event
"""

from enum import Enum
import asyncio

class RWLock:
    """A multiple read/single write lock asynchronous implementation of asyncio lock providing
    locking context with specific lock policy (read/write)
    """

    class Permissions(Enum):
        """Three possible states:
        none
        read
        write
        """

        none = 0
        read = 1
        write = 2

    class _Context:
        """Read lock context
        """

        def __init__(self, afunc, efunc):
            self.__afunc = afunc
            self.__efunc = efunc
            self.__fake_await_event = asyncio.Event()
            self.__fake_await_event.set()

        async def __aenter__(self):
            if self.__afunc is not None:
                await self.__afunc()

        async def __aexit__(self, *args, **kwargs):
            if self.__efunc is not None:
                await self.__fake_await_event.wait()
                self.__efunc()

    def __init__(self):
        self.__wlock_request = False
        self.__wevent = asyncio.Event()
        self.__wevent.set()

        self.__revent = asyncio.Event()
        self.__revent.set()
        self.__readers_count = 0

    def context(self, permission: Permissions):
        """Create guard context with `permission` guard

        Args:
            permission: The permission to lock with
        """

        if permission == self.Permissions.read:
            return self.read_context()
        elif permission == self.Permissions.write:
            return self.write_context()

        # This context does nothing
        return self._Context(None, None)

    def read_context(self):
        """Create guard context with read guard
        """

        return self._Context(self.r_lock, self.r_unlock)

    def write_context(self):
        """Create guard context with write guard
        """

        return self._Context(self.lock, self.unlock)

    async def r_lock(self):
        """Lock the guard for reading, can be multiple readers
        if lock has been called, wait until unlock is done
        """

        await self.__wevent.wait()
        self.__readers_count += 1
        if self.__revent.is_set():
            self.__revent.clear()

    async def lock(self):
        """Lock the guard for writing when there is no more reader
        r_lock calls will now wait for the write lock to be unlocked
        """

        if not self.__wevent.is_set():
            raise RuntimeError("Lock has already been write locked")
        self.__wevent.clear()
        await self.__revent.wait()

    def r_unlock(self):
        """Unlock the guard for reading, if there is no more reader, unlock the lock for writing
        """

        if not self.__readers_count:
            raise RuntimeError("Can't read unlock an lock with zero readers")

        self.__readers_count -= 1
        if self.__readers_count == 0:
            self.__revent.set()

    def unlock(self):
        """Unlock the guard for writing, waiting readers are notified and can acquire the lock
        """

        self.__wevent.set()
