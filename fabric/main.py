import apps.db as db
import apps.misp as misp
import asyncio
import logging
from backbone import InMemoryBackbone
from fabric import Fabric

async def main():
    backbone = InMemoryBackbone()
    fabric = Fabric(backbone)

    # run all the apps in parallel
    db_task = asyncio.create_task(db.start(fabric))
    misp_task = asyncio.create_task(misp.start(fabric))
    await db_task
    await misp_task

if __name__ == "__main__":
    asyncio.run(main())
