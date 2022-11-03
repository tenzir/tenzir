import asyncio

from dynaconf import Dynaconf
import stix2

from vast import VAST
import vast.fabric.apps.thehive as thehive
import vast.utils.asyncio
import vast.utils.config
import vast.utils.logging


async def start(config: Dynaconf):
    vast = await VAST.create(config)
    loop = asyncio.get_event_loop()
    loop.create_task(thehive.start(vast))
    loop.create_task(vast.republish("suricata.alert"))


def main():
    config = vast.utils.config.parse()
    vast.utils.logging.configure(config.logging)
    vast.utils.asyncio.run_forever(start(config))


if __name__ == "__main__":
    main()
