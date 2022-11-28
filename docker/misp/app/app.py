import asyncio
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import AsyncIterable

import pymisp
import vast.utils.logging as logging
import zmq
import zmq.asyncio

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


async def pull_zmq() -> AsyncIterable[str]:
    """Connect to the MISP ZMQ feed and create a raw string iterator out of it"""
    socket = zmq.asyncio.Context().socket(zmq.SUB)
    logger.info(f"connecting to 0mq endpoint at {MISP_ZMQ_URL}")
    socket.connect(MISP_ZMQ_URL)
    socket.setsockopt(zmq.SUBSCRIBE, b"misp_json")
    try:
        while True:
            raw = await socket.recv()
            raw_str = raw.decode("utf-8")
            logger.debug(raw_str)
            yield raw_str
    finally:
        logger.info(f"terminating")
        socket.setsockopt(zmq.LINGER, 0)
        socket.close()


async def published_misp_events(
    zmq_messages: AsyncIterable[str],
) -> AsyncIterable[pymisp.MISPEvent]:
    """Filter and parse published events out of the MISP ZMQ stream.

    MISP publishes:
    - misp_json_event messages when interacting with the event entity
    - misp_json_attribute messages when creating attributes
    - misp_json messages when clicking on "Publish Event"
    This method filters published events only (misp_json) and parses them into
    pymisp.MISPEvent objects."""
    async for raw_message in zmq_messages:
        # decode raw zmq message and skip everything except misp_json_event
        event_name, json_payload = raw_message.split(" ", 1)
        if event_name != "misp_json":
            continue
        payload = json.loads(json_payload)
        event = payload.get("Event", None)
        if event is None:
            logger.warning(f"Unexpected payload for misp_json_event: {event}")
            continue
        # parse payload as pymisp.MISPEvent
        try:
            misp_event = pymisp.MISPEvent()
            misp_event.from_dict(**event)
            yield misp_event
        except Exception as e:
            logger.warning(f"Failed to parse ZMQ message as MISP event: {e}")
            continue


async def run_async():
    async for event in published_misp_events(pull_zmq()):
        logger.info(event.to_json())
        # TODO: handle event


def run():
    logger.info("Starting MISP app...")
    asyncio.run(run_async())
    logger.info("MISP app stopped")
