from typing import Callable, Awaitable, Any, List, Dict
from asyncio import run_coroutine_threadsafe

from rocketmq.client import PushConsumer, ConsumeStatus

from .utils import check_group_id

class Subscription:
    def __init__(self, topic: str, expression: str,
                 handler: Callable[..., Awaitable[Any]],
                 depth: int = 0):

        self._topic = topic
        self._expression = expression
        self._handler = handler
        self._decorated_depth = depth
        self._consumer = None


    def subscribe(self, prefix, host, loop):

        if not prefix.endswith("%"):
            prefix += "%"

        handler = self._handler
        ## group_id不建议含有函数行号，避免因程序修改导致groupid频繁修改
        group_id = (
            f"{prefix}{handler.__module__}%{handler.__qualname__}"
            f"%{self._decorated_depth}"
        )

        group_id.replace(".", "%")

        check_group_id(group_id)


        consumer = PushConsumer(group_id=group_id)
        consumer.set_thread_count(1)
        consumer.set_name_server_address(host)
        _subscribe_handler(loop, consumer, self._topic,
                           self._expression, handler)

        consumer.start()
        self._consumer = consumer

    def unsubscribe(self):
        if self._consumer:
            self._consumer.shutdown()
            self._consumer = None



def _subscribe_handler(loop, consumer, topic, expression, handler):
    def _callback(msg):
        message_id = msg.id
        message = msg.body
        tags = msg.tags
        try:
            corou = handler(message_id, message)
            future = run_coroutine_threadsafe(corou, loop)
            _result = future.result()
            return ConsumeStatus.CONSUME_SUCCESS
        except Exception as exc:
            return ConsumeStatus.RECONSUME_LATER

    consumer.subscribe(topic, _callback, expression=expression)

