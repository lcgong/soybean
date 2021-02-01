import asyncio
from soybean import RocketMQChannel

from sqlblock import AsyncPostgresSQL

channel = RocketMQChannel("demo", "127.0.0.1:9876")
dbconn = AsyncPostgresSQL(dsn="postgresql://postgres@localhost/postgres")

order_topic = channel.topic("Order")

@order_topic.react("Commit")
async def on_order_commiting(message_id, message):
    print(f"Commit: got message '{message_id}': {message}")

@order_topic.react("Reviewed")
async def on_order_reviewed(message_id, message: bytes):
    print(f"Reviewed: got message '{message_id}': {message}")

# message.putUserProperties(PropertyKeyConst.CheckImmunityTimeInSeconds,"120")


@order_topic.action("Commit")
@dbconn.transaction
async def order_commit_action(order):
    """
    如果action的函数是一个事务性的函数，则完成action之后所发送的消息是事务性消息，
    以保证事务和所发送的消息是一致性的，事务和消息发送要么最终都以成功，要么都失败。
    """
    dbconn.sql("""SELECT 1 AS sn""")
    row = await dbconn.fetch_first()
    # await asyncio.sleep(120)
    order["sn"] = row.sn
    return order

@order_commit_action.recheck
async def handler(msg):
    """
    消息回查: 如果消息超时未收到commit,则重新检查.
    
    如果已经事务已经提交则返回True，如果事务失败，返回False
    
    """
    print("rechecker order_commit ")
    return True

@order_topic.action("Reviewed")
async def evaluate_risk(order):
    order["risk_rating"] = 5
    return order


async def main():
    async with dbconn, channel:
        await asyncio.sleep(0.1)
        try:
            order = {"name": "Tom"}
            order = await order_commit_action(order)
            order = await evaluate_risk(order)
            print("ret: ", order)
        except Exception as exc:
            print("ERROR: ", exc)
            raise exc

        while True:
            await asyncio.sleep(60)

try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass
