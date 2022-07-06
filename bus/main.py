import apps.db as db
import apps.misp as misp
import asyncio


async def main():
    # run all the apps in parallel
    db_task = asyncio.create_task(db.start())
    misp_task = asyncio.create_task(misp.start())

    await db_task
    await misp_task


if __name__ == "__main__":
    asyncio.run(main())
