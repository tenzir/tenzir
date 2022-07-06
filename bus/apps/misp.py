import asyncio
from bus import VASTBus


async def start():
    print("VAST DB App started")
    vast = VASTBus()
    await vast.publish("greeting-channel", {"message": "hello misp"})
    while True:
        await asyncio.sleep(1)
