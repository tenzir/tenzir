import asyncio
from fabric import Fabric


async def start(vast: Fabric):
    print("VAST DB App started")
    await vast.publish("greeting-topic", {"message": "hello misp"})
    while True:
        await asyncio.sleep(1)
