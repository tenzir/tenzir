import asyncio


async def start():
    print("VAST DB App started")
    while True:
        await asyncio.sleep(1)
