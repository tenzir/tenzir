from bus import VASTBus
import asyncio


async def start():
    print("VAST DB App started")
    vast = VASTBus()
    await vast.publish("greeting-channel", {"message": "hello vast db"})
    while True:
        await asyncio.sleep(1)
