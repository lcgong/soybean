import asyncio
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

from .subscription import Subscription
from .utils import check_topic_name


_registry: Dict[Callable[..., Awaitable[Any]], List[Subscription]] = {}


class RocketMQChannel:

    def __init__(self, name, host: str = None) -> None:
        self._channel_name = name
        self._name_srv_addrs = host
        self._subscriptions = []

    @property
    def channel_name(self):
        return self._channel_name

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
    def __init__(self,  channel: RocketMQChannel, topic: str):
        self._channel = channel
        self._topic = topic

    def listen(self, expression: str = "*") -> Any:
        def _decorator(handler: Callable[..., Awaitable[Any]]): 
            return self._channel.subscribe(self._topic, expression)(handler)
        
        return _decorator


