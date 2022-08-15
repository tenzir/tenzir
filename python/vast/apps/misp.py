import json
from typing import Any

import misp_stix_converter
import pymisp
import stix2
import zmq
import zmq.asyncio

from vast import VAST
import vast.bridges.stix as vbs
import vast.utils.logging

logger = vast.utils.logging.get(__name__)

# The MISP app.
class MISP:
    """The app for MISP.

    It hooks into the 0mq feed and interacts with the MISP instance via
    PyMISP."""

    def __init__(self, vast: VAST):
        self.vast = vast
        self.config = vast.config.apps.misp
        self.stix_bridge = vbs.STIX()
        logger.info(f"connecting to REST API at {self.config.api.host}")
        try:
            self.misp = pymisp.ExpandedPyMISP(
                url=self.config.api.host, key=self.config.api.key, ssl=False
            )
        except pymisp.exceptions.PyMISPError as e:
            logger.error(f"failed to connect: {e}")

    async def run(self):
        await self.vast.fabric.pull(self._on_bundle)
        await self.vast.fabric.pull(self._on_sighting)
        # Hook into event feed via 0mq.
        socket = zmq.asyncio.Context().socket(zmq.SUB)
        zmq_uri = f"tcp://{self.config.zmq.host}:{self.config.zmq.port}"
        logger.info(f"connecting to 0mq endpoint at {zmq_uri}")
        socket.connect(zmq_uri)
        # TODO: also subscribe to attributes.
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
                    await self.vast.fabric.push(parser.bundle)
                except Exception as e:
                    logger.warning(f"failed to parse MISP event as STIX: {e}")
                    continue
        finally:
            logger.info(f"terminating")
            self.misp = None
            socket.setsockopt(zmq.LINGER, 0)
            socket.close()

    async def _on_sighting(self, sighting: stix2.Sighting):
        logger.debug(f"got {sighting}")
        # For every Sighting, do the following:
        # 1. Check the embedded sightee if it was a already MISP object.
        # 2. If not, extract the UUID and ask MISP whether it maps to an
        #    existing object.
        # 3. Take the value of the attached SCOs and register them as sightings,
        #    with the timestamp being from the Observed Data SDO.
        sightee = self.stix_bridge.store.get(sighting.sighting_of_ref)
        misp_uuid = vbs.make_uuid(sightee.id)
        data = self.misp.search(uuid=misp_uuid, controller="attributes")
        logger.warning(data)
        # MISP performs a UUID-preserving conversion of attributes into
        # either Indicator or Observed Data.
        if (type(sightee) == stix2.Indicator):
            pass
        elif (type(sightee) == stix2.ObservedData):
            pass
        else:
            logger.warning(f"ignoring sightee of type {type(sightee)}")
            continue


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
                    type="0",  # true positive
                    timestamp=11111111,
                    source="VAST",
                )
                sightings.append(sighting)
        for sighting in sightings:
            response = self.misp.add_sighting(sighting)
            if not response or type(response) is dict and response.get("message", None):
                logger.error(
                    f"failed to add sighting to MISP: '{sighting}' ({response})"
                )
            else:
                logger.info(f"reported sighting: {response}")


async def start(vast: VAST):
    misp = MISP(vast)
    await misp.run()
