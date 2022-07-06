import apps.test as test
import asyncio
from backbone import InMemoryBackbone
from fabric import Fabric


async def main():
    backbone = InMemoryBackbone()
    vast = Fabric(backbone)
    await test.start(vast)


if __name__ == "__main__":
    asyncio.run(main())
