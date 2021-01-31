import socket
import os
import asyncio
from asyncio import run_coroutine_threadsafe
from rocketmq.client import PushConsumer, ConsumeStatus
from typing import Callable, Awaitable, Any, List, Dict


"""
在消息队列中，GroupId目的维持在并发条件下消费位点(offset)的一致性。
管理每个消费队列的不同消费组的消费进度是一个非常复杂的事情。消息会被多个消费者消费，
不同的是每个消费者只负责消费其中部分消费队列，添加或删除消费者，都会使负载发生变动，
容易造成消费进度冲突，因此需要集中管理。
因此，RocketMQ为了简化问题，尤其是集群消费模式时，采用远程模式管理offset，并且限制主题和标签组合。
这即RocketMQ所谓的。


消息处理函数位置作为和主题和tag定义作为一个唯一的group_id。

"""
import inspect


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


_registry: Dict[Callable[..., Awaitable[Any]], List[Subscription]] = {}


class Channel:

    def __init__(self, host: str = None) -> None:
        self._name_srv_addrs = host
        self._subscriptions = []

    def topic(self, name: str):
        check_topic_name(name)

        return _TopicPort(self, name)

    def subscribe(self, topic: str, expression: str = '*'):

        def _decorator(handler: Callable[..., Awaitable[Any]]):

            handler_subscriptions = _registry.get(handler)
            if handler_subscriptions is None:
                handler_subscriptions = []
                _registry[handler] = handler_subscriptions

            subscription = Subscription(topic, expression, handler, len(handler_subscriptions))
            handler_subscriptions.append(subscription)
            self._subscriptions.append(subscription)

            return handler

        return _decorator

    async def start(self):
        if len(self._subscriptions) == 0:
            return

        loop = asyncio.get_running_loop()
        for listener in self._subscriptions:
            listener.subscribe("demo", self._name_srv_addrs, loop)

        print("started")

    async def stop(self):
        for listener in self._subscriptions:
            listener.unsubscribe()
        print("stopped")

    async def __aenter__(self):
        await self.start()

    async def __aexit__(self, exec_type, value, traceback):
        await self.stop()



class _TopicPort:
    def __init__(self,  channel: Channel, topic: str):
        self._channel = channel
        self._topic = topic

    def __call__(self, expression: str = "*") -> Any:
        def _decorator(handler: Callable[..., Awaitable[Any]]): 
            return self._channel.subscribe(self._topic, expression)(handler)
        
        return _decorator



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

import re

VALID_NAME_PATTERN = re.compile("^[%|a-zA-Z0-9_-]+$")
VALID_NAME_STR = (
    "allowing only numbers, uppercase and lowercase letters," 
    " '%', '|', '-' and '_' symbols"
)

class InvalidTopicName(Exception):
    ...

class InvalidGroupId(Exception):
    ...

def check_topic_name(name):
    if not name:
        raise InvalidTopicName("The topic name is empty")

    if not VALID_NAME_PATTERN.match(name):
        raise InvalidTopicName(f"the topic name '{name}' contains illegal characters, {VALID_NAME_STR}")

    if len(name) > 127:
        raise InvalidTopicName("the topic name is longer than name max length 127.")

def check_group_id(name):
    if not name:
        raise InvalidGroupId("The group_id is empty")

    if not VALID_NAME_PATTERN.match(name):
        raise InvalidGroupId(f"the group_id '{name}' contains illegal characters, {VALID_NAME_STR}")

    if len(name) > 255:
        raise InvalidGroupId("the group_id is longer than name max length 255.")
