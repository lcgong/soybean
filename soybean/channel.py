import asyncio
from typing import Callable, Awaitable, Any, List, Dict
from typing import ForwardRef
import functools

from .reactor import Reactor
from .utils import check_topic_name, pinyin_translate
from .typing import HandlerType
from .action.simple import SendingAction, SimpleAction
from .action.transactional import TransactionalAction


"""
在消息队列中，GroupId目的维持在并发条件下消费位点(offset)的一致性。
管理每个消费队列的不同消费组的消费进度是一个非常复杂的事情。消息会被多个消费者消费，
不同的是每个消费者只负责消费其中部分消费队列，添加或删除消费者，都会使负载发生变动，
容易造成消费进度冲突，因此需要集中管理。
因此，RocketMQ为了简化问题，尤其是集群消费模式时，采用远程模式管理offset，并且限制主题和标签组合。
这即RocketMQ所谓的。


消息处理函数位置作为和主题和tag定义作为一个唯一的group_id。

"""

TopicChannel = ForwardRef("_TopicPort")


class DomainChannel:
    __slots__ = (
        "_name",
        "_namesrv_addr",
        "_producers",
        "_reactors",
        "_loop",
    )

    def __init__(self, domain, namesrv_addr):
        self._name = domain
        self._namesrv_addr = namesrv_addr
        self._producers = {}
        self._reactors = {}

    @property
    def name(self):
        return self._name

    @property
    def namesrv_addr(self):
        return self._namesrv_addr

    def topic(self, name: str) -> TopicChannel:
        name = pinyin_translate(name)
        check_topic_name(name)

        return TopicChannel(self, topic=name)

    def get_producer(self, group_id):
        return self._producers.get(group_id)

    def get_reactor(self, group_id):
        return self._reactors.get(group_id)

    def register_producer(self, group_id, producer):
        self._producers[group_id] = producer

    def register_reactor(self, group_id, reactor):
        self._reactors[group_id] = reactor

    def get_running_loop(self):
        return self._loop

    async def start(self):
        self._loop = asyncio.get_running_loop()

        for reactor in self._reactors.values():
            await reactor.start()

        for producer in self._producers.values():
            producer.start()

    async def stop(self):
        for producer in self._producers.values():
            producer.shutdown()

        for reactor in self._reactors.values():
            await reactor.stop()



class RocketMQ:
    __slots__ = ("_channel",)

    def __init__(self,  domain:str, namesrv_addr: str ="localhost:9876"):
        self._channel = DomainChannel(domain, namesrv_addr)
    
    def topic(self, topic: str) -> TopicChannel:
        return self._channel.topic(topic)

    @property
    def domain_name(self) -> str:
        return self._channel._name

    @property
    def namesrv_addr(self) -> str:
        return self._channel._namesrv_addr

    async def start(self):
        await self._channel.start()

    async def stop(self):
        await self._channel.stop()

    async def __aenter__(self):
        await self.start()

    async def __aexit__(self, exec_type, value, traceback):
        await self.stop()


class TopicChannel:
    def __init__(self,  channel: DomainChannel, topic: str):
        self._channel = channel
        self._topic = topic

    def react(self, expression: str = "*") -> Any:
        def _decorator(handler: HandlerType):

            reactors = getattr(handler, "__reactors__", None)
            if reactors is None:
                reactors = []
                setattr(handler, "__reactors__", reactors)

            reactor = Reactor(self._channel,
                              self._topic, expression,
                              handler, depth=len(reactors))
            self._channel.register_reactor(reactor.reactor_id, reactor)

        return _decorator

    async def send(self, msg: Any,
             key: str = None,
             tag: str = None,
             orderly=False,
             props: Dict[str, str] = None):

        action = SendingAction(self._channel,
                               self._topic, tag,
                               orderly=orderly, props=props)
        await action.send(msg)

    def action(self, tag=None, orderly=False, props=None):
        def _decorator(handler):

            sqlblock_meta = getattr(handler, "__sqlblock_meta__", None)
            if sqlblock_meta is not None:
                # transactional action
                action = TransactionalAction(
                    self._channel,
                    sqlblock_meta._wrapped_func,
                    sqlblock_meta._database,
                    self._topic, tag, props)

                async def _wrapped_action(*args, **kwargs):
                    return await action.execute(*args, **kwargs)

                def _rechecker_decorator(handler):
                    action._rechecker = handler

                setattr(_wrapped_action, "recheck", _rechecker_decorator)
                functools.update_wrapper(_wrapped_action, handler)
                return _wrapped_action

            else:
                # the simple action
                action = SimpleAction(self._channel, handler, self._topic, tag,
                                      orderly=orderly, props=props)

                async def _wrapped_action(*args, **kwargs):
                    return await action.execute(*args, **kwargs)

                functools.update_wrapper(_wrapped_action, handler)
                return _wrapped_action

        return _decorator
