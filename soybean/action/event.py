import threading
import asyncio

class ThreadingEventValue:

    def __init__(self, initial=None):
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._value = initial

    def wait(self):
        self._event.wait()
        with self._lock:
            return self._value

    def get(self):
        with self._lock:
            return self._value

    def set(self, value):
        with self._lock:
            self._value = value
            self._event.set()


class AsyncEventValue:

    def __init__(self, value=None):
        self._event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._value = value

    async def wait(self):
        await self._event.wait()
        async with self._lock:
            return self._value

    async def get(self):
        async with self._lock:
            return self._value

    async def set(self, value):
        async with self._lock:
            self._value = value
            self._event.set()
