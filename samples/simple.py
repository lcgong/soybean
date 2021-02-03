import asyncio
from soybean import RocketMQ

channel = RocketMQ("demo", "127.0.0.1:9876")

order_topic = channel.topic("MyOrder")

@order_topic.react("提交订单")
async def commit_order(msg_id, message):
    print(f"A: got message '{msg_id}': {message}")


@order_topic.react("Pay")
async def pay_order(msg_id, message):
    print(f"B: got message '{msg_id}': {message}")


async def biz_action1():
    for i in range(8):
        msg = {"name": f"name-{i:03d}"}
        tag = "提交订单" if i % 2 == 0 else "Pay"
        await order_topic.send(msg, key=f"P{i:03d}", tag=tag, orderly=True)

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
