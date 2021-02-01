import inspect
from asyncio import run_coroutine_threadsafe
from rocketmq.client import PushConsumer, ConsumeStatus

from .utils import make_group_id, json_loads
from .typing import HandlerType


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

    @property
    def reactor_id(self):
        return self._reactor_id

    async def start(self):

        consumer = PushConsumer(group_id=self._reactor_id)

        consumer.set_thread_count(1)
        consumer.set_name_server_address(self._channel.namesrv_addr)

        loop = self._channel.get_running_loop()

        reactor_func = self._handler

        def _callback(msg):
            argvals = self._handler_argvals_getter(msg)
            try:
                coro = reactor_func(*argvals)
                future = run_coroutine_threadsafe(coro, loop)
                future.result()
                return ConsumeStatus.CONSUME_SUCCESS
            except Exception:
                return ConsumeStatus.RECONSUME_LATER

        consumer.subscribe(self._topic, _callback, expression=self._expression)
        consumer.start()

        self._consumer = consumer

    async def stop(self):
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


class UnkownArgumentError(ValueError):
    pass

