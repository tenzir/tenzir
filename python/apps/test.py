import asyncio
from fabric import Fabric, Message


async def test_callback(msg):
    print("test_callback got msg:", msg.to_bytes().decode())


async def start(vast: Fabric):
    print("Test App started")
    await vast.subscribe("welcoming-topic", test_callback)
    await vast.subscribe("farewelling-topic", test_callback)
    await vast.publish("welcoming-topic", Message.from_bytes(b"hello"))
    await vast.publish("welcoming-topic", Message.from_bytes(b"world"))
    await vast.publish("farewelling-topic", Message.from_bytes(b"goodbye"))
    await vast.publish("farewelling-topic", Message.from_bytes(b"buddy"))
