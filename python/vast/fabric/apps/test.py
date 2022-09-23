from fabric import Fabric


async def test_callback(msg):
    print("test_callback got msg:", msg)


async def start(fabric: Fabric):
    print("Test App started")
    await fabric.subscribe("welcoming-topic", test_callback)
    await fabric.subscribe("farewelling-topic", test_callback)
    await fabric.publish("welcoming-topic", "hello")
    await fabric.publish("welcoming-topic", "world")
    await fabric.publish("farewelling-topic", "goodbye")
    await fabric.publish("farewelling-topic", "buddy")
