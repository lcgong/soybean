import asyncio
from typing import Callable, Awaitable, Any, List, Dict
from typing import ForwardRef

from .subscription import Subscription
from .utils import check_topic_name
from .messenger import Messenger
from .typing import HandlerType

"""
在消息队列中，GroupId目的维持在并发条件下消费位点(offset)的一致性。
管理每个消费队列的不同消费组的消费进度是一个非常复杂的事情。消息会被多个消费者消费，
不同的是每个消费者只负责消费其中部分消费队列，添加或删除消费者，都会使负载发生变动，
容易造成消费进度冲突，因此需要集中管理。
因此，RocketMQ为了简化问题，尤其是集群消费模式时，采用远程模式管理offset，并且限制主题和标签组合。
这即RocketMQ所谓的。


消息处理函数位置作为和主题和tag定义作为一个唯一的group_id。

"""

_TopicPort = ForwardRef("_TopicPort")


class RocketMQChannel:

    def __init__(self, name: str, host: str = None) -> None:
        self._channel_name = name
        self._name_srv_addrs = host
        self._subscriptions = []
        self._messenger = Messenger(self)

    def topic(self, name: str) -> _TopicPort:
        check_topic_name(name)

        return _TopicPort(self, name)

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        self._messenger._loop = loop

        if len(self._subscriptions) == 0:
            return

        for listener in self._subscriptions:
            listener.subscribe(self._channel_name, self._name_srv_addrs, loop)

        

        print("started")

    async def stop(self) -> None:
        for listener in self._subscriptions:
            listener.unsubscribe()

        await self._messenger.stop()

        print("stopped")

    async def __aenter__(self):
        await self.start()

    async def __aexit__(self, exec_type, value, traceback):
        await self.stop()

# import typing as t
# JSONType = t.Union[str, int, float, bool, None, t.Dict[str, t.Any], t.List[t.Any]]


class _TopicPort:
    def __init__(self,  channel: RocketMQChannel, topic: str):
        self._channel = channel
        self._topic = topic

    def react(self, expression: str = "*") -> Any:
        def _decorator(handler: HandlerType):
            subscription = Subscription(self._topic, expression, handler)
            self._channel._subscriptions.append(subscription)

            return handler

        return _decorator

    def send_json(self, msg: Any,
                  key: str = None,
                  tag: str = None,
                  orderly=False,
                  props: Dict[str, str] = None):

        messenger = self._channel._messenger
        messenger.send_json(self._topic, msg,
                            key=key,
                            tag=tag,
                            orderly=orderly,
                            props=props)

    def action(self, tag=None, orderly=False):
        messenger = self._channel._messenger

        return messenger.send_in_transaction(self._topic, tag=tag, orderly=orderly)
