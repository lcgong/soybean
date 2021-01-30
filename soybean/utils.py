import re
from .exceptions import InvalidTopicName, InvalidGroupId

VALID_NAME_PATTERN = re.compile("^[%|a-zA-Z0-9_-]+$")
VALID_NAME_STR = (
    "allowing only numbers, uppercase and lowercase letters,"
    " '%', '|', '-' and '_' symbols"
)


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
