import asyncio
import json
import logging
import time
import os
from typing import AsyncIterable

import misp_stix_converter
import pymisp
import stix2
import zmq
import zmq.asyncio
from concurrent.futures import ThreadPoolExecutor

import vast.utils.logging as logging
import vast.utils.stix

logger = logging.get("vast.thehive.app")

MISP_HTTP_URL = os.environ["MISP_HTTP_URL"]
MISP_ZMQ_URL = os.environ["MISP_ZMQ_URL"]
MISP_API_KEY = os.environ["MISP_API_KEY"]
THREADPOOL = ThreadPoolExecutor()


async def misp_client() -> pymisp.ExpandedPyMISP:
    start_time = time.time()
    loop = asyncio.get_running_loop()
    while True:
        try:
            create = lambda: pymisp.ExpandedPyMISP(
                url=MISP_HTTP_URL, key=MISP_API_KEY, ssl=False
            )
            return await loop.run_in_executor(THREADPOOL, create)
        except Exception as e:
            if time.time() - start_time > 120:
                raise e
            logger.debug(e)
            await asyncio.sleep(1)


# async def _on_stix(bundle: stix2.Bundle):
#     logger.debug(f"got {bundle}")
#     # Make it easier to look up referenced objects.
#     store = stix2.MemoryStore()
#     store.add(bundle)
#     misp_sightings = []
#     for object in bundle.objects:
#         match type(object):
#             # For every Sighting, do the following:
#             # 1. Check the embedded "sightee" if it was already a MISP object.
#             # 2. If not, extract the UUID and ask MISP whether it maps to an
#             #    existing object.
#             # 3. Take the value of the attached SCOs and register them as sightings,
#             #    with the timestamp being from the Observed Data SDO.
#             case stix2.Sighting:
#                 # Ad (1): misp-stix embeds MISP meta data in STIX objects.
#                 # find it.
#                 sightee = store.get(object.sighting_of_ref)
#                 logger.info(sightee)
#                 match type(sightee):
#                     case stix2.Indicator:
#                         pass
#                     case stix2.ObservedData:
#                         pass
#                     case _:
#                         logger.warning(f"not a valid sightee: {type(sightee)}")

#                 # Ad (2): Nothing found locally, MISP may not have associated objects.
#                 misp_uuid = vast.utils.stix.make_uuid(sightee.id)
#                 data = MISP.search(uuid=misp_uuid, controller="attributes")
#                 logger.warning(data)
# FIXME: this a shortcut. We'll go through the bundle and assume that
# all SCOs are sightings.
# for object in bundle.objects:
#    # TODO: get timestamp from out Observed Data.
#    if type(object) is stix2.IPv4Address:
#        sighting = pymisp.MISPSighting()
#        sighting.from_dict(
#            value=object.value,
#            type="0",  # true positive
#            timestamp=11111111,
#            source="VAST",
#        )
#        misp_sightings.append(sighting)
# for sighting in misp_sightings:
#    response = self.misp.add_sighting(sighting)
#    if not response or type(response) is dict and response.get("message", None):
#        logger.error(
#            f"failed to add sighting to MISP: '{sighting}' ({response})"
#        )
#    else:
#        logger.info(f"reported sighting: {response}")


async def pull_zmq() -> AsyncIterable[stix2.Bundle]:
    # Hook into event feed via 0mq.
    socket = zmq.asyncio.Context().socket(zmq.SUB)
    logger.info(f"connecting to 0mq endpoint at {MISP_ZMQ_URL}")
    socket.connect(MISP_ZMQ_URL)
    # TODO: also subscribe to attributes, not just entire events.
    socket.setsockopt(zmq.SUBSCRIBE, b"misp_json")
    try:
        while True:
            raw = await socket.recv()
            _, message = raw.decode("utf-8").split(" ", 1)
            json_msg = json.loads(message)
            logger.debug(json_msg)
            event = json_msg.get("Event", None)
            attribute = json_msg.get("Attribute", None)
            # Only consider events, not Attributes that ship within Events.
            if not event or attribute:
                continue
            parser = misp_stix_converter.MISPtoSTIX21Parser()
            try:
                parser.parse_misp_event(event)
                logger.debug(parser.bundle.serialize(pretty=True))
                yield parser.bundle
            except Exception as e:
                logger.warning(f"failed to parse MISP event as STIX: {e}")
                continue
    finally:
        logger.info(f"terminating")
        socket.setsockopt(zmq.LINGER, 0)
        socket.close()


async def run_async():
    async for bundle in pull_zmq():
        print(bundle)


def run():
    logger.info("Starting TheHive app...")
    asyncio.run(run_async())
    logger.info("TheHive app stopped")
