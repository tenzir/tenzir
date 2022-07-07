import apps.test as test
import asyncio
import pyvast
from backbone import InMemoryBackbone
from fabric import Fabric


async def main():
    backbone = InMemoryBackbone()
    vast = Fabric(backbone)
    await test.start(vast)


async def pyvasttest():
    vast = pyvast.VAST(container={"runtime": "docker", "name": "vast-pro"})
    await vast.test_connection()
    proc = await vast.count().exec()
    stdout, stderr = await proc.communicate()
    print("stdout", stdout)
    print("stderr", stderr)


if __name__ == "__main__":
    asyncio.run(main())
    # asyncio.run(test())
