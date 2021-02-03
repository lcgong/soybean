
import re
import os
import socket
from sys import modules
from sqlblock.utils.json import json_dumps, json_loads
from rocketmq.client import Message

from .exceptions import InvalidGroupId, InvalidTopicName

VALID_NAME_PATTERN = re.compile("^[%|a-zA-Z0-9_-]+$")
VALID_NAME_STR = (
    "allowing only numbers, uppercase and lowercase letters,"
    " '%', '|', '-' and '_' symbols"
)


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


def check_topic_name(name):
    if not name:
        raise InvalidTopicName("The topic name is empty")

    if not VALID_NAME_PATTERN.match(name):
        raise InvalidTopicName(
            f"the topic name '{name}' contains illegal characters, {VALID_NAME_STR}")

    if len(name) > 127:
        raise InvalidTopicName(
            "the topic name is longer than name max length 127.")


def check_group_id(name):
    if not name:
        raise InvalidGroupId("The group_id is empty")

    if not VALID_NAME_PATTERN.match(name):
        raise InvalidGroupId(
            f"the group_id '{name}' contains illegal characters, {VALID_NAME_STR}")

    if len(name) > 255:
        raise InvalidGroupId(
            "the group_id is longer than name max length 255.")


def make_group_id(channel_name, handler_func, depth=None):

    channel_name = pinyin_translate(channel_name)

    module_name = handler_func.__module__.replace(".", "-")
    func_name = handler_func.__qualname__.replace(".", "-")

    module_name = pinyin_translate(module_name)
    func_name = pinyin_translate(func_name)

    depth = f"-{depth}" if depth is not None else ""

    return f"{channel_name}%{module_name}-{func_name}{depth}"


def make_instance_id():
    return f"{socket.gethostname()}_{os.getpid()}"

from pypinyin import NORMAL as NORMAL_PINYIN
from pypinyin.converter import DefaultConverter
from pypinyin.seg.simpleseg import seg as pinyin_seg

def pinyin_translate(s):
    """
    将字符串中含有的中文字符转换成拼音，每个中文的拼音采用驼峰拼接，如‘中文‘转换为‘ZhongWen’.
    """
    converter = DefaultConverter()
    segments = pinyin_seg(s)

    translated = []
    for s in segments:
        if not s:
            continue

        tt = converter.convert(s, NORMAL_PINYIN,
                        heteronym=False,
                        errors='default',
                        strict=True)
        t = tt[0][0]
        if s == t:
            translated.append(s)
        else:
            translated.append(t[0].upper() + t[1:])
    
    return "".join(translated)
            


