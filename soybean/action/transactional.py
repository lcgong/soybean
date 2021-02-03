from asyncio import run_coroutine_threadsafe
from rocketmq.client import SendStatus
from rocketmq.client import TransactionMQProducer, TransactionStatus
from concurrent.futures import ThreadPoolExecutor

from ..typing import HandlerType
from ..exceptions import TrasnactionPreparingError
from ..utils import make_group_id
from ..event import ThreadingEventValue, AsyncEventValue
from . import make_action_msg

producer_executor = ThreadPoolExecutor(max_workers=5)

# from ..channel import Channel

class TransactionalAction:

    def __init__(self,
                 channel,
                 handler: HandlerType,
                 sqlblock_database,
                 topic: str,
                 tag: str = None,
                 props=None):

        self._channel = channel
        self._handler = handler
        self._sqlblock_database = sqlblock_database
        self._topic = topic
        self._tag = tag
        self._props = props
        self._rechecker = None

        self._group_id = make_group_id(channel.name, handler)

    def get_producer(self):
        producer = self._channel.get_producer(self._group_id)
        if producer is not None:
            return producer

        loop = self._channel._loop
        if self._rechecker:
            _rechecker = self._rechecker
        else:
            _rechecker = _default_rechecker

        def _recheck_callback(msg):
            future = run_coroutine_threadsafe(_rechecker(msg), loop)
            try:
                is_success = future.result()
                if not isinstance(is_success, bool):
                    print("rechecker should return a True or False value")
                    return TransactionStatus.UNKNOWN

                if is_success:
                    return TransactionStatus.COMMIT
                else:
                    return TransactionStatus.ROLLBACK
            except Exception as exc:
                print("rechecker error: ", str(exc))
                return TransactionStatus.UNKNOWN

        producer = TransactionMQProducer(self._group_id, _recheck_callback)
        producer.set_name_server_address(self._channel.namesrv_addr)
        producer.start()


        self._channel.register_producer(self._group_id, producer)
        return producer

    async def execute(self, *handler_args, **handler_kwargs):
        message = TransactionalMessage(self)

        @self._sqlblock_database.transaction
        async def _transactional_handler():
            # 在事务内执行内在逻辑
            result = await self._handler(*handler_args, **handler_kwargs)
            await message.prepare(result) # 事务提交前发送准备消息
            return result

        try:
            ret = await _transactional_handler()
            await message.confirm() # 事务已提交，发送确认，允许事务消息发送
            return ret
        except Exception as exc:
            message.exception(exc) # 事务回滚，撤回消息
            raise


async def _default_rechecker(msg):
    return True


class TransactionalMessage:
    def __init__(self, action: TransactionalAction):
        self._action = action
        self._transaction_status = ThreadingEventValue(
            TransactionStatus.UNKNOWN)

    async def prepare(self, action_result):

        action = self._action
        loop = action._channel.get_running_loop()
        msg_obj = make_action_msg(action_result,
                                  action._topic,
                                  action._tag)

        prepared = AsyncEventValue()

        def _send(producer):

            def _local_execute(msg, user_args):
                run_coroutine_threadsafe(prepared.set(SendStatus.OK), loop)
                # return TransactionStatus.UNKNOWN
                return self._transaction_status.wait()

            try:
                ret = producer.send_message_in_transaction(
                    msg_obj, _local_execute, None)
                if ret.status != SendStatus.OK:
                    run_coroutine_threadsafe(prepared.set(ret.status), loop)

            except Exception as exc:
                run_coroutine_threadsafe(prepared.set(exc), loop)

        producer_executor.submit(_send, action.get_producer())

        send_status = await prepared.wait()
        if send_status == SendStatus.OK:
            return

        if send_status == SendStatus.FLUSH_DISK_TIMEOUT:
            raise TrasnactionPreparingError("flush disk timeout")
        elif send_status == SendStatus.FLUSH_SLAVE_TIMEOUT:
            raise TrasnactionPreparingError("flush slave timeout")
        elif send_status == SendStatus.SLAVE_NOT_AVAILABLE:
            raise TrasnactionPreparingError("slave not available")
        else:
            raise TrasnactionPreparingError(str(send_status))

    async def confirm(self):
        self._transaction_status.set(TransactionStatus.COMMIT)

    def exception(self, exc):
        self._transaction_status.set(TransactionStatus.ROLLBACK)
