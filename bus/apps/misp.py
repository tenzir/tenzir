import asyncio
from bus import VASTBus


async def start(vast: VASTBus):
    print("VAST DB App started")
    await vast.publish("greeting-topic", {"message": "hello misp"})
    while True:
        await asyncio.sleep(1)
