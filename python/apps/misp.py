import json
from typing import Any

import misp_stix_converter
import pymisp
import stix2
import zmq
import zmq.asyncio

from vast import VAST
import utils.logging

logger = utils.logging.get(__name__)

# The MISP app.
class MISP:
    def __init__(self, vast: VAST):
        self.vast = vast
        self.config = vast.config.apps.misp
        logger.info(f"connecting to REST API at {self.config.api.host}")
        try:
            self.misp = pymisp.ExpandedPyMISP(
                url=self.config.api.host,
                key=self.config.api.key,
                ssl=False)
        except pymisp.exceptions.PyMISPError as e:
            logger.error(f"failed to connect: {e}")

    async def run(self):
        await self.vast.subscribe("stix.bundle", self._on_bundle)
        # Hook into event feed via 0mq.
        socket = zmq.asyncio.Context().socket(zmq.SUB)
        zmq_uri = f"tcp://{self.config.zmq.host}:{self.config.zmq.port}"
        logger.info(f"connecting to 0mq endpoint at {zmq_uri}")
        socket.connect(zmq_uri)
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
                    logger.info(parser.bundle.serialize(pretty=True))
                    await self.vast.publish("stix.bundle", parser.bundle)
                except Exception as e:
                    logger.warning(f"failed to parse MISP event as STIX: {e}")
                    continue
        finally:
            logger.info(f"terminating MISP")
            self.misp = None
            socket.setsockopt(zmq.LINGER, 0)
            socket.close()

    async def _on_bundle(self, message: Any):
        logger.debug(f"got bundle: {message}")
        bundle = stix2.parse(message)
        if type(bundle) != stix2.Bundle:
            logger.warn(f"expected bundle, got {type(bundle)}")
            return
        sightings = []
        # FIXME: this a shortcut. We'll go through the bundle and assume that
        # all SCOs are sightings.
        for object in bundle.objects:
            # TODO: get timestamp from out Observed Data.
            if type(object) is stix2.IPv4Address:
                sighting = pymisp.MISPSighting()
                sighting.from_dict(
                        value=object.value,
                        type="0", # true positive
                        timestamp=11111111,
                        source="VAST",
                        )
                sightings.append(sighting)
        for sighting in sightings:
            response = self.misp.add_sighting(sighting)
            if not response or type(response) is dict and response.get("message", None):
               logger.error(f"failed to add sighting to MISP: '{sighting}' ({response})")
            else:
                logger.info(f"reported sighting: {response}")


async def start(vast: VAST):
    misp = MISP(vast)
    await misp.run()
