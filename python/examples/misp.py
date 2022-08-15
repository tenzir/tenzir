import asyncio

from dynaconf import Dynaconf
import stix2

from vast import VAST
import vast.apps.misp as misp
import vast.utils.asyncio
import vast.utils.config
import vast.utils.logging


async def start(config: Dynaconf):
    vast = await VAST.create(config)
    loop = asyncio.get_event_loop()
    loop.create_task(misp.start(vast))

    # Put an Indicator on the fabric to trigger a query in VAST, which in turn
    # sends the sightings to MISP.
    ind = stix2.Indicator(
        description="Test", pattern_type="vast", pattern='"CQishF25ynsGkC6v6e"'
    )
    await vast.fabric.publish("stix.indicator", ind)


def main():
    config = vast.utils.config.parse()
    vast.utils.logging.configure(config.logging)
    vast.utils.asyncio.run_forever(start(config))


if __name__ == "__main__":
    main()
