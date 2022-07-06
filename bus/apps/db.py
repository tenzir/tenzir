from bus import VASTBus
import asyncio


async def start(vast: VASTBus):
    print("VAST DB App started")
    await vast.publish("greeting-topic", {"message": "hello vast db"})
    while True:
        await asyncio.sleep(1)
