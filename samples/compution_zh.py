import asyncio
import soybean

channel = soybean.RocketMQ("soybean_samples", "127.0.0.1:9876")

order_topic = channel.topic("奇数偶数之和")

finshed_compuation = soybean.Event()

@order_topic.action("结果")
async def 开始计算():

    message = {
        "步数": 0,
        "数值": 0,
        "结果": 0 
    }

    return message

@order_topic.action("结果")
async def 执行偶计算(message):

    message["数值"] = message["步数"] * 2
    print(f"执行偶计算: {message}")
    return message


@order_topic.action("结果")
async def 执行奇计算(message):

    message["数值"] = message["步数"] * 2 + 1
    print(f"执行奇计算: {message}")
    return message


@order_topic.react("结果")
async def on_计算(message):
    
    message["结果"] += message["数值"]
    print(f"结果: {message}")

    if message["步数"] > 6:
        finshed_compuation.set()
        return

    message["步数"] +=  1
    if message["步数"] % 2 == 0:
        await 执行偶计算(message)
    else:
        await 执行奇计算(message)

async def main():
    async with channel:
        await 开始计算()
        await finshed_compuation.wait()

try:
    asyncio.run(main(), debug=True)
except KeyboardInterrupt:
    pass
