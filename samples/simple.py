import asyncio
from soybean import RocketMQChannel

channel = RocketMQChannel("demo", "127.0.0.1:9876")

order_topic = channel.topic("Order")

@order_topic.listen("Commit")
async def commit_order(msg_id, msg):
    print(f"A: got message '{msg_id}': {msg}")


@order_topic.listen("Pay")
async def pay_order(msg_id, msg):
    print(f"B: got message '{msg_id}': {msg}")


async def biz_action1():
    for i in range(8):
        msg = {"name": f"name-{i:03d}"}
        tag = "Commit" if i % 2 == 0 else "Pay"
        order_topic.send_json(msg, key=f"P{i:03d}", tag=tag)

async def main():
    async with channel:
        await asyncio.sleep(1)
        await biz_action1()

        while True:
            await asyncio.sleep(60)

try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass
