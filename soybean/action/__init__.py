
from ..utils import create_jsonobj_msg

def make_action_msg(result, topic, tag):
    msg_key = None
    return create_jsonobj_msg(topic, result, msg_key, tag)
