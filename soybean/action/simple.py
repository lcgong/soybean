from rocketmq.client import SendStatus
from rocketmq.client import Producer

from ..typing import HandlerType
from ..exceptions import ActionError
from . import make_action_msg

# from ..channel import Channel


class SendingAction:
    def __init__(self,
                 channel,
                 topic: str,
                 tag: str = None,
                 orderly: bool = False,
                 props=None):

        self._channel = channel
        self._topic = topic
        self._tag = tag
        self._orderly = orderly
        self._props = props

        group_id = f"{channel.name}"
        if orderly:
            group_id += "|orderly"
        self._group_id = group_id

    def get_producer(self):

        producer = self._channel.get_producer(self._group_id)
        if producer is not None:
            return producer

        producer = Producer(self._group_id, orderly=self._orderly)
        producer.set_name_server_address(self._channel.namesrv_addr)
        producer.start()

        self._channel.register_producer(self._group_id, producer)
        return producer

    async def send(self, msg):
        msg_obj = make_action_msg(msg, self._topic, self._tag)

        try:
            producer = self.get_producer()
            if self._orderly:
                response = producer.send_orderly_with_sharding_key(
                    msg_obj, sharding_key="")
            else:
                response = producer.send_sync(msg_obj)
        except Exception as exc:
            raise ActionError(str(exc)) from exc

        response_status = response.status
        if response_status == SendStatus.OK:
            return msg

        if response_status == SendStatus.FLUSH_DISK_TIMEOUT:
            raise ActionError("flush disk timeout")
        elif response_status == SendStatus.FLUSH_SLAVE_TIMEOUT:
            raise ActionError("flush slave timeout")
        elif response_status == SendStatus.SLAVE_NOT_AVAILABLE:
            raise ActionError("slave not available")
        else:
            raise ActionError("unknow send status code")


class SimpleAction(SendingAction):

    def __init__(self,
                 channel,
                 handler: HandlerType,
                 topic: str,
                 tag: str = None,
                 orderly: bool = False,
                 props=None):

        super().__init__(channel, topic, tag, orderly, props)

        self._handler = handler

    async def execute(self, *handler_args, **handler_kwargs):
        ret_val = await self._handler(*handler_args, **handler_kwargs)
        await self.send(ret_val)
        return ret_val
