import asyncio

from dynaconf import Dynaconf
import stix2

from vast import VAST, Fabric
import vast.apps.misp as misp
import vast.apps.misp as zeek
import vast.utils.asyncio
import vast.utils.config
import vast.utils.logging


async def start(config: Dynaconf):
    vast = VAST(config)
    fabric = Fabric(config)
    loop = asyncio.get_event_loop()
    loop.create_task(misp.start(fabric, vast))
    loop.create_task(zeek.start(fabric, vast))
    # Put an Indicator on the fabric to trigger a query in VAST, which in turn
    # sends the sightings to MISP.
    indicator = stix2.Indicator(
        description="A VAST query that checks for a known Zeek connection UID",
        pattern_type="vast",
        pattern='"CQishF25ynsGkC6v6e"',
    )
    await fabric.push(stix2.Bundle(indicator))


def main():
    config = vast.utils.config.parse()
    vast.utils.logging.configure(config.logging)
    vast.utils.asyncio.run_forever(start(config))


if __name__ == "__main__":
    main()
