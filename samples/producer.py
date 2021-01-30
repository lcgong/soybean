from rocketmq.client import Producer, Message

producer = Producer('PID-XXX')
producer.set_name_server_address('127.0.0.1:9876')
producer.start()

from datetime import datetime

ts = datetime.now().isoformat()

for i in range(10):
    tag = "Commit" if i % 2 == 0 else "Pay"
    msg = Message('Order_1')
    msg.set_keys(f"{ts}##{i+1:03d}")
    msg.set_tags(tag)
    msg.set_body(f"msg[{ts}##{i+1:03d}] dddsd")
    ret = producer.send_sync(msg)
    print(ret.status, tag, ret.msg_id, ret.offset)

producer.shutdown()
