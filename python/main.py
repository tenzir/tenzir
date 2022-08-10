import asyncio

from dynaconf import Dynaconf
import stix2

import apps.misp
import utils.asyncio
import utils.config
import utils.logging
from vast import VAST

async def start(config: Dynaconf):
    vast = await VAST.create(config)
    #proc = await vast.start(db_directory="/tmp/vast.db")
    #await proc.communicate()

    loop = asyncio.get_event_loop()
    loop.create_task(apps.misp.start(vast))

    ind = stix2.Indicator(
            description="Test",
            pattern_type="vast",
            pattern="\"CQishF25ynsGkC6v6e\"")
    await vast.publish("stix.indicator", ind)

def main():
    config = utils.config.parse()
    utils.logging.configure(config.logging)
    utils.asyncio.run_forever(start(config))

if __name__ == "__main__":
    main()
