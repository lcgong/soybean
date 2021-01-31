
import re
import os
import socket
from sys import modules


from .exceptions import InvalidGroupId, InvalidTopicName

VALID_NAME_PATTERN = re.compile("^[%|a-zA-Z0-9_-]+$")
VALID_NAME_STR = (
    "allowing only numbers, uppercase and lowercase letters," 
    " '%', '|', '-' and '_' symbols"
)


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

def make_group_id(prefix, handler, sn=0):

    module_name = handler.__module__.replace(".", "%")
    func_name = handler.__qualname__.replace(".", "%")

    return f"{prefix}%{module_name}%{func_name}-{sn}"


def make_instance_id():
    return f"{socket.gethostname()}_{os.getpid()}"