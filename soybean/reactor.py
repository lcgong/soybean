import inspect
import asyncio
import logging
from rocketmq.client import PushConsumer, ConsumeStatus

from .utils import make_group_id, json_loads
from .event import OccupiedEvent
from .typing import HandlerType
from .exceptions import UnkownArgumentError

logger = logging.getLogger("soybean.reactor")

class Reactor:
    def __init__(self, channel, topic: str, expression: str,
                 handler: HandlerType, depth: int):

        self._channel = channel
        self._topic = topic
        self._expression = expression
        self._handler = handler

        self._reactor_id = make_group_id(channel.name, handler, depth)
        self._consumer = None

        argvals_getter = build_argvals_getter(handler)
        self._handler_argvals_getter = argvals_getter

        self._busy_event = None

    @property
    def reactor_id(self):
        return self._reactor_id

    async def start(self):
        import threading
        print(
            f"reacter-start thread: {threading.get_ident()}, loop: {id(asyncio.get_event_loop())}")

        consumer = PushConsumer(group_id=self._reactor_id)

        consumer.set_thread_count(1)
        consumer.set_name_server_address(self._channel.namesrv_addr)

        self._busy_event = OccupiedEvent()

        loop = asyncio.get_running_loop()
        def run_coroutine(coroutine):
            # 在其它线程以线程安全的方式执行协程，并阻塞等待执行结果
            future = asyncio.run_coroutine_threadsafe(coroutine, loop)
            return future.result

        def _callback(msg):
            run_coroutine(self._busy_event.acquire())
            try:
                arg_values = self._handler_argvals_getter(msg)
                run_coroutine( self._handler(*arg_values))

                return ConsumeStatus.CONSUME_SUCCESS
            except Exception as exc:
                logger.error((f"caught an error in reactor "
                              f"'{self._reactor_id}': {exc}"),
                             exc_info=exc)
                return ConsumeStatus.RECONSUME_LATER
            finally:
                run_coroutine(self._busy_event.release())

        consumer.subscribe(self._topic, _callback, expression=self._expression)
        consumer.start()

        self._consumer = consumer

    async def stop(self):
        await self._busy_event.wait_idle()

        # 问题：当前rocket-client-cpp实现在shutdown之前并不能保证工作线程正常结束
        # 这会导致工作线程和asyncio死锁，所以得到callback线程里任务结束后，再多等待
        # 一会儿，等待rocket-client-cpp处理完consumer工作线程，再关闭consumer
        await asyncio.sleep(0.5)

        if self._consumer:
            self._consumer.shutdown()
            self._consumer = None


def build_argvals_getter(handler):
    arguments = inspect.signature(handler).parameters

    getters = []
    unknowns = []
    for arg_name, arg_spec in arguments.items():
        getter_factory = _getter_factories.get(arg_name)
        if getter_factory is not None:
            getters.append(getter_factory(arg_spec))
            continue

        unknowns.append((arg_name, arg_spec))

    if unknowns:
        mod = handler.__module__
        func = handler.__qualname__
        args = ", ".join([f"'{name}'" for name, spec in unknowns])
        errmsg = f"Unknown arguments: {args} of '{func}' in '{mod}'"
        raise UnkownArgumentError(errmsg)

    def _getter(msgobj):
        return (arg_getter(msgobj) for arg_getter in getters)

    return _getter


def getter_message(arg_spec):
    if arg_spec.annotation == str:
        return lambda msgobj: msgobj.body.decode("utf-8")
    elif arg_spec.annotation == bytes:
        return lambda msgobj: msgobj.body
    else:
        return lambda msgobj: json_loads(msgobj.body.decode("utf-8"))


def getter_msg_id(arg_spec):
    return lambda msgobj: getattr(msgobj, "id")


def getter_msg_topic(arg_spec):
    return lambda msgobj: getattr(msgobj, "tpoic").decode("utf-8")


def getter_msg_keys(arg_spec):
    return lambda msgobj: getattr(msgobj, "keys").decode("utf-8")


def getter_msg_tags(arg_spec):
    return lambda msgobj: getattr(msgobj, "tags").decode("utf-8")


_getter_factories = {
    "message": getter_message,
    "message_id": getter_msg_id,
    "message_topic": getter_msg_topic,
    "message_keys": getter_msg_keys,
    "message_tags": getter_msg_tags,
    "msg_id": getter_msg_id,
    "msg_topic": getter_msg_topic,
    "msg_keys": getter_msg_keys,
    "msg_tags": getter_msg_tags,
}

