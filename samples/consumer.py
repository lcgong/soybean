import asyncio
from soybean import RocketMQChannel

channel = RocketMQChannel('127.0.0.1:9876')

order_topic = channel.topic("Order")

@order_topic.listen(tag="Commit")
async def commit_order(msg_id, msg):
    print(f"A: got message '{msg_id}': {msg}")


@order_topic.listen(tag="Pay")
async def pay_order(msg_id, msg):
    print(f"B: got message '{msg_id}': {msg}")


async def main():
    async with channel:
        while True: 
            await asyncio.sleep(60)

try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass
