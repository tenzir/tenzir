import fabric
import asyncio
import pyvast
import logging
import json

import stix2
#from stix2 import parse, Indicator, Sighting
from vast_threatbus.message_mapping import (indicator_to_vast_matcher_ioc, matcher_result_to_sighting)

logger = logging.getLogger("db-app")


class DB:
    def __init__(self):
        self.db = pyvast.VAST(binary="../build/bin/vast")
        self.matcher_name = "vast-app-matcher"

    async def test_connection(self):
        return asyncio.create_task(self.db.test_connection())

    async def attach_matcher(self, topic):
        proc = await self.db.matcher().attach().json(self.matcher_name).exec()
        # returncode is None as long as the process did not terminate yet
        while proc.returncode is None:
            data = await proc.stdout.readline()
            if not data:
                if not await self.db.test_connection():
                    logger.error("Lost connection to VAST, cannot live-match")
                    # TODO reconnect
                continue
            vast_sighting = data.decode("utf-8").rstrip()
            sighting = matcher_result_to_sighting(vast_sighting)
            if not sighting:
                logger.error(f"Cannot parse sighting-output from VAST: {vast_sighting}")
                continue
            # g_live_matcher_sightings.inc()
            logger.info(f"Got a new sighting from VAST")
            await sightings_queue.put(sighting)
        stderr = await proc.stderr.read()
        if stderr:
            logger.error(
                "VAST matcher process exited with message: {}".format(stderr.decode())
            )
        logger.critical("Unexpected exit of VAST matcher process.")

    async def handle_sighting(self, message: fabric.Message):
        global logger
        logger.info(f"got sighting {message.to_bytes()}")
        indicator = stix2.parse(message.to_bytes())
        ioc = indicator_to_vast_matcher_ioc(indicator)
        # Trigger new retro match
        retro_match = db.export(max_events=10).json(ioc["value"]).exec()
        # Add the ioc to the matcher
        matcher_addition = await db.matcher().add(self.matcher_name, ioc["value"], ioc["reference"]).exec()
        for result in retro_match:
            vast.publish("vast.sighting", result)

async def start(vast: fabric.Fabric):
    global logger
    db = DB()
    await db.test_connection()
    logger.info("VAST DB App started")
    await db.attach_matcher("vast.sighting")
    await vast.subscribe("stix.indicator", db.handle_sighting)
    # Wait forever
    cond = asyncio.Condition()
    async with cond:
        await cond.wait()
