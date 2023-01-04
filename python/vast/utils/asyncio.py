import asyncio
from signal import SIGINT, SIGTERM, SIGHUP

import vast.utils.logging

logger = vast.utils.logging.get(__name__)


async def shutdown(loop, signal=None):
    """Cleanup tasks tied to the service's shutdown."""
    if signal:
        logger.info(f"received exit signal {signal.name}")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    logger.debug(f"cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


def handle_exception(loop, context):
    # context["message"] will always be there, but context["exception"] may not
    msg = context.get("exception", context["message"])
    logger.error(f"caught exception: {msg}")
    logger.info("shutting down...")
    asyncio.create_task(shutdown(loop))


def configure(loop):
    logger.debug("registering signal handler for SIGINT and SIGTERM")
    stop = lambda: asyncio.create_task(shutdown(loop, signal))
    for signal in [SIGINT, SIGTERM, SIGHUP]:
        loop.add_signal_handler(signal, stop)
    loop.set_exception_handler(handle_exception)


def run_forever(task):
    loop = asyncio.get_event_loop()
    configure(loop)
    try:
        loop.create_task(task)
        loop.run_forever()
    finally:
        loop.close()
        logger.info("terminated successfully")
