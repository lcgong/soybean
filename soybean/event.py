import collections
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


class OccupiedEvent:
    """占用事件. 

    acquire()标记资源已被占用，release()清除占用标记。
    wait_idle()如果资源被占用则阻塞等待，直到资源空闲，没有任何协程标记占用。
    """
    def __init__(self):
        self._lock = asyncio.Lock()
        self._occupied_count = 0

        # initial state is idle
        idle_event = asyncio.Event()
        idle_event.set()
        self._idle_event = idle_event

    async def acquire(self):
        async with self._lock:
            self._occupied_count += 1
            if self._occupied_count == 1:
                self._idle_event.clear()

    async def release(self):
        assert self._occupied_count > 0
        async with self._lock:
            self._occupied_count -= 1
            if self._occupied_count == 0:
                self._idle_event.set()

    async def wait_idle(self):
        await self._idle_event.wait()

class Event:
    """
    使用非常像asyncio.Event，与其不同，只有在wait才获取正在运行的loop，避免提早创建loop导致死锁。

    一个Event对象，wait()阻塞当前线程等待，直到该对象被set()标记过才执行；
    如果在wait()之前已经被set()标记过,则无需组赛等待。clear()清除set标记.
    """

    def __init__(self):
        self._waiters = collections.deque()
        self._value = False

    def __repr__(self):
        res = super().__repr__()
        extra = 'set' if self._value else 'unset'
        if self._waiters:
            extra = f'{extra}, waiters:{len(self._waiters)}'
        return f'<{res[1:-1]} [{extra}]>'

    def is_set(self):
        """是否已被标记过"""
        return self._value

    def set(self):
        """
        进行标记。所有之前等待该标记协程都会被唤醒。之后wait()无需阻塞等待。
        """
        if not self._value:
            self._value = True

            for fut in self._waiters:
                if not fut.done():
                    fut.set_result(True)

    def clear(self):
        """清除标记。之后执行wait()的协程会被阻塞。"""
        self._value = False

    async def wait(self):
        """阻塞等待直到被被标记。

        如果已经标记立即返回True；之后被其它协程标记，则被唤醒。
        """
        if self._value:
            return True

        fut = asyncio.get_running_loop().create_future()
        self._waiters.append(fut)
        try:
            await fut
            return True
        finally:
            self._waiters.remove(fut)
