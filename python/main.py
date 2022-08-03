import argparse
import asyncio
from signal import SIGINT, SIGTERM, SIGHUP

from dynaconf import Dynaconf

from fabric import Fabric
import apps.misp
import backbones.inmemory
import utils.logger

logger = utils.logger.create()

async def shutdown(loop, signal=None):
    """Cleanup tasks tied to the service's shutdown."""
    if signal:
        logger.info(f"received exit signal {signal.name}")
    tasks = [t for t in asyncio.all_tasks() if t is not
             asyncio.current_task()]
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

def main():
    # Parse config file(s).
    configs = ["config.yaml", "config.yml"]
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", help="path to a configuration file")
    args = parser.parse_args()
    if args.config:
        configs = [args.config]
    config = Dynaconf(
        settings_files=configs,
        load_dotenv=True,
        envvar_prefix="VAST",
    )
    # Setup logger.
    utils.logger.configure(config.logging, logger)
    # Configure event loop to exit gracefully on CTRL+C.
    loop = asyncio.get_event_loop()
    logger.debug("registering signal handler for SIGINT and SIGTERM")
    stop = lambda: asyncio.create_task(shutdown(loop, signal))
    for signal in [SIGINT, SIGTERM, SIGHUP]:
        loop.add_signal_handler(signal, stop)
    loop.set_exception_handler(handle_exception)
    try:
        # Spawn the fabric.
        logger.debug("initializing fabric")
        backbone = backbones.inmemory.InMemory()
        fabric = Fabric(config, backbone)
        # Spawn apps.
        logger.debug("spawning apps")
        loop.create_task(apps.misp.start(config, fabric))
        loop.run_forever()
    finally:
        loop.close()
        logger.info("successfully shutdown VAST")

if __name__ == "__main__":
    main()
