from asyncio import run_coroutine_threadsafe
from rocketmq.client import PushConsumer, ConsumeStatus

from .utils import make_group_id
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
            message_id = msg.id
            message = msg.body
            # tags = msg.tags
            try:
                coro = reactor_func(message_id, message)
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
