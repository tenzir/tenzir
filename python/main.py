from dynaconf import Dynaconf
import argparse
import asyncio
import logging
from signal import SIGINT, SIGTERM
import sys

from fabric import Fabric
import apps.misp
import backbones.inmemory

async def main(config):
    # Spawn the fabric.
    backbone = backbones.inmemory.InMemory()
    fabric = Fabric(config, backbone)
    # Spawn plugins.
    try:
        await asyncio.gather(apps.misp.start(config, fabric))
    except asyncio.CancelledError:
        pass # TODO: log

if __name__ == "__main__":
    # Parse config file
    settings_files = ["config.yaml", "config.yml"]
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", "-c", help="path to a configuration file")
    args = parser.parse_args()
    if args.config:
        if not args.config.endswith("yaml") and not args.config.endswith("yml"):
            sys.exit("please provide a `yaml` or `yml` configuration file.")
        settings_files = [args.config]
    config = Dynaconf(
        settings_files=settings_files,
        load_dotenv=True,
        envvar_prefix="VAST",
    )
    # Setup logger.
    # TODO: do nicely with colors.
    logging.basicConfig(level = logging.DEBUG)
    # Configure event loop to exit gracefully on CTRL+C.
    loop = asyncio.get_event_loop()
    main_task = asyncio.ensure_future(main(config))
    for signal in [SIGINT, SIGTERM]:
        loop.add_signal_handler(signal, main_task.cancel)
    try:
        loop.run_until_complete(main_task)
    finally:
        loop.close()
