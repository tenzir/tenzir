import asyncio

from dynaconf import Dynaconf
import stix2

import apps.misp
import apps.thehive
import utils.asyncio
import utils.config
import utils.logging
from vast import VAST

async def start(config: Dynaconf):
    vast = await VAST.create(config)
    #proc = await vast.start(db_directory="/tmp/vast.db")
    #await proc.communicate()

    loop = asyncio.get_event_loop()
    #loop.create_task(apps.misp.start(vast))
    loop.create_task(apps.thehive.start(vast))

    #ind = stix2.Indicator(
    #        description="Test",
    #        pattern_type="vast",
    #        pattern="\"CQishF25ynsGkC6v6e\"")
    #await vast.fabric.publish("stix.indicator", ind)

    #async def async_print(x):
    #    print(x)
    #    return

    #await vast.fabric.subscribe("suricata.alert", async_print)

    loop.create_task(vast.republish("suricata.alert"))

def main():
    config = utils.config.parse()
    utils.logging.configure(config.logging)
    utils.asyncio.run_forever(start(config))

if __name__ == "__main__":
    main()
