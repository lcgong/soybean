import asyncio
from asyncio import run_coroutine_threadsafe
from multiprocessing.spawn import prepare
from soybean import channel

from rocketmq.client import Producer, Message, SendStatus
from rocketmq.client import TransactionMQProducer, TransactionStatus
from typing import Callable, Awaitable, Any, List, Dict
from sqlblock.utils.json import json_dumps
from functools import update_wrapper as update_func_wrapper
from .utils import make_group_id
from .exceptions import SendingError, TrasnactionPreparingError
import threading
from concurrent.futures import ThreadPoolExecutor


class Messenger:

    def __init__(self, channel) -> None:
        self._producers = {}
        self._name_srv_addrs = channel._name_srv_addrs
        self._channel_name = channel._channel_name
        self._loop = None

    def _get_producer(self, orderly=False):

        group_id = self._channel_name
        if orderly:
            group_id += "_orderly"

        producer = self._producers.get(group_id)
        if producer is None:
            producer = Producer(group_id, orderly=orderly)
            self._producers[group_id] = producer

            producer.set_name_server_address(self._name_srv_addrs)
            producer.start()

        return producer

    def send_json(self, topic, msg,
                  key: str = None,
                  tag: str = None,
                  orderly=False,
                  props: Dict[str, str] = None):

        producer = self._get_producer(orderly=orderly)

        msg_obj = Message(topic)
        if isinstance(key, str):
            msg_obj.set_keys(key.encode("utf-8"))

        if isinstance(tag, str):
            msg_obj.set_tags(tag.encode("utf-8"))

        if isinstance(props, dict):
            for k, v in props.items():
                msg_obj.set_property(k, v)

        msg_obj.set_body(json_dumps(msg).encode("utf-8"))

        ret = producer.send_sync(msg_obj)
        if ret.status > 0:
            if ret.status == 1:
                raise SendingError("flush disk timeout")
            elif ret.status == 2:
                raise SendingError("flush slave timeout")
            elif ret.status == 3:
                raise SendingError("slave not available")

        return (ret.msg_id, ret.offset)

    def send_transaction(self, sqlblock_conn, topic, tag=None, props=None):
        
        def _decorator(handler):
            executor = TransactionExecutor(handler, self, sqlblock_conn, topic, tag, props)

            async def _wrapped_handler(*args, **kwargs):
                return await executor.execute(*args, **kwargs)
        
            def _rechecker_decorator(handler):
                executor._rechecker = handler


            setattr(_wrapped_handler, "recheck", _rechecker_decorator)
            update_func_wrapper(_wrapped_handler, handler)
            
            return _wrapped_handler

        return _decorator


    async def start(self):
        ...

    async def stop(self):
        for producer in self._producers.values():
            producer.shutdown()


class TransactionExecutor:

    def __init__(self, handler, messenger: Messenger, sqlblock_conn, topic, tag=None, props=None):
        self._handler = handler
        self._rechecker = None

        self._messenger = messenger
        self._sqlblock_conn = sqlblock_conn
        self._topic = topic
        self._tag = tag
        self._props = props
        self._producer = None

        channel_name = messenger._channel_name
        self._group_id = make_group_id(channel_name, handler, sn=0)

    def get_producer(self):

        if self._producer is not None:
            return self._producer

        loop = self._messenger._loop
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
        producer.set_name_server_address(self._messenger._name_srv_addrs)
        producer.start()

        self._messenger._producers[self._group_id] = producer

        self._producer = producer
        return producer


    async def execute(self, *handler_args, **handler_kwargs):
        message = TransactionMessage(self)

        @self._sqlblock_conn.transaction
        async def _transactional_handler():
            # 在事务内执行内在逻辑
            result = await self._handler(*handler_args, **handler_kwargs)
            await message.prepare(result)
            return result

        try:
            ret = await _transactional_handler()
            await message.confirm()
            return ret
        except Exception as exc:
            message.exception(exc)
            raise


async def _default_rechecker(msg):
    return True

producer_executor = ThreadPoolExecutor(max_workers=5)


class TransactionMessage:
    def __init__(self, executor: TransactionExecutor):
        self._executor = executor
        self.prepared = False

        self._lock = threading.Lock()
        self._event = threading.Event()
        self._transaction_status = TransactionStatus.UNKNOWN

    def get_transaction_status(self):
        self._event.wait()
        with self._lock:
            return self._transaction_status

    def set_transaction_status(self, status):
        with self._lock:
            self._transaction_status = status

        self._event.set()

    async def prepare(self, action_result):

        loop = self._executor._messenger._loop
        if isinstance(action_result, tuple) and len(action_result) == 2:
            msg_key, jsonobj = action_result
        else:
            msg_key = None
            jsonobj = action_result

        topic = self._executor._topic
        tag = self._executor._tag

        msg_obj = create_jsonobj_msg(topic, jsonobj, msg_key, tag)

        prepared = AsyncEventValue()

        def _send_transaction_message(producer):

            def _local_execute(msg, user_args):
                run_coroutine_threadsafe(prepared.set(SendStatus.OK), loop)
                # return TransactionStatus.UNKNOWN
                return self.get_transaction_status()

            try:
                ret = producer.send_message_in_transaction(
                    msg_obj, _local_execute, None)
                if ret.status != SendStatus.OK:
                    run_coroutine_threadsafe(prepared.set(ret.status), loop)

            except Exception as exc:
                run_coroutine_threadsafe(prepared.set(exc), loop)
        
        producer = self._executor.get_producer()
        producer_executor.submit(_send_transaction_message, producer)

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
        self.set_transaction_status(TransactionStatus.COMMIT)

    def exception(self, exc):
        self.set_transaction_status(TransactionStatus.ROLLBACK)


class AsyncEventValue:

    def __init__(self, value=None):
        self._event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._value = value

    async def wait(self):
        await self._event.wait()
        async with self._lock:
            return self._value

    async def get(self):
        async with self._lock:
            return self._value

    async def set(self, value):
        async with self._lock:
            self._value = value
            self._event.set()
        print("SET_EVENT: ", value)


def create_jsonobj_msg(topic, jsonobj, key=None, tag=None, props=None):
    msg_obj = Message(topic)
    if isinstance(key, str):
        msg_obj.set_keys(key.encode("utf-8"))

    if isinstance(tag, str):
        msg_obj.set_tags(tag.encode("utf-8"))

    if isinstance(props, dict):
        for k, v in props.items():
            msg_obj.set_property(k, v)

    msg_obj.set_body(json_dumps(jsonobj).encode("utf-8"))

    return msg_obj
