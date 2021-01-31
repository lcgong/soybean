import asyncio
from soybean import RocketMQChannel

from sqlblock import AsyncPostgresSQL

channel = RocketMQChannel("demo", "127.0.0.1:9876")
dbconn = AsyncPostgresSQL(dsn="postgresql://postgres@localhost/postgres")

order_topic = channel.topic("Order")

@order_topic.listen("Commit")
async def on_commit_order(msg_id, msg):
    print(f"A: got message '{msg_id}': {msg}")


@order_topic.send_transaction(dbconn, "Commit")
async def order_commit_action(order):
    """
    保证事务和消息发送的一致性.

    事务完成后

    """
    dbconn.sql("""SELECT 1 AS sn""")
    row = await dbconn.fetch_first()
    # await asyncio.sleep(120)
    order["sn"] = row.sn
    return order

@order_commit_action.recheck
async def handler(msg):
    """
    如果消息超时未收到commit,则重新检查.
    
    如果已经事务已经提交则返回True，如果事务失败，返回False
    
    """
    print("rechecker order_commit ")
    return True

async def main():
    async with dbconn, channel:
        await asyncio.sleep(0.1)
        try:
            ret = await order_commit_action({"name": "Tom"})
            print("ret: ", ret)
        except Exception as exc:
            print("ERROR: ", exc)
            raise exc

        while True:
            await asyncio.sleep(60)

try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass
