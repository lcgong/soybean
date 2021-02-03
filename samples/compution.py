import asyncio
import soybean

domain = soybean.RocketMQ("soybean_samples", "127.0.0.1:9876")
topic = domain.topic("OddEvenComputation")

compuation_finshed = soybean.Event()

@topic.action("Result")
async def start_computation():
    return {"step": 0, "value": 0, "result": 0}

@topic.action("Result")
async def compute_at_even_step(message):

    message["value"] = message["step"] * 2
    print(f"{'step_even':>10s}: {message}")
    return message


@topic.action("Result")
async def compute_at_odd_step(message):

    message["value"] = message["step"] * 2 + 1
    print(f"{'step_odd':>10s}: {message}")
    return message


@topic.react("Result")
async def on_result(message):
    
    message["result"] += message["value"]
    print(f"{'Result':>10s}: {message}")

    if message["step"] > 6:
        compuation_finshed.set()
        return

    message["step"] +=  1
    if message["step"] % 2 == 0:
        await compute_at_even_step(message)
    else:
        await compute_at_odd_step(message)

async def main():
    async with domain:
        await start_computation()
        await compuation_finshed.wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
