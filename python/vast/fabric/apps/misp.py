import json

import misp_stix_converter
import pymisp
import stix2
import zmq
import zmq.asyncio

from vast import Fabric
import vast.utils.logging
import vast.utils.stix

logger = vast.utils.logging.get(__name__)

# The MISP app.
class MISP:
    """The app for MISP.

    It hooks into the 0mq feed and interacts with the MISP instance via
    PyMISP."""

    def __init__(self, fabric: Fabric):
        self.fabric = fabric
        self.config = fabric.config.apps.misp
        logger.info(f"connecting to REST API at {self.config.api.host}")
        try:
            self.misp = pymisp.ExpandedPyMISP(
                url=self.config.api.host, key=self.config.api.key, ssl=False
            )
        except pymisp.exceptions.PyMISPError as e:
            logger.error(f"failed to connect: {e}")

    async def run(self):
        await self.fabric.pull(self._on_stix)
        # Hook into event feed via 0mq.
        socket = zmq.asyncio.Context().socket(zmq.SUB)
        zmq_uri = f"tcp://{self.config.zmq.host}:{self.config.zmq.port}"
        logger.info(f"connecting to 0mq endpoint at {zmq_uri}")
        socket.connect(zmq_uri)
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
                    await self.fabric.push(parser.bundle)
                except Exception as e:
                    logger.warning(f"failed to parse MISP event as STIX: {e}")
                    continue
        finally:
            logger.info(f"terminating")
            self.misp = None
            socket.setsockopt(zmq.LINGER, 0)
            socket.close()

    async def _on_stix(self, bundle: stix2.Bundle):
        logger.debug(f"got {bundle}")
        # Make it easier to look up referenced objects.
        store = stix2.MemoryStore()
        store.add(bundle)
        misp_sightings = []
        for object in bundle.objects:
            match type(object):
                # For every Sighting, do the following:
                # 1. Check the embedded "sightee" if it was already a MISP object.
                # 2. If not, extract the UUID and ask MISP whether it maps to an
                #    existing object.
                # 3. Take the value of the attached SCOs and register them as sightings,
                #    with the timestamp being from the Observed Data SDO.
                case stix2.Sighting:
                    # Ad (1): misp-stix embeds MISP meta data in STIX objects.
                    # find it.
                    sightee = store.get(object.sighting_of_ref)
                    logger.info(sightee)
                    match type(sightee):
                        case stix2.Indicator:
                            pass
                        case stix2.ObservedData:
                            pass
                        case _:
                            logger.warning(f"not a valid sightee: {type(sightee)}")

                    # Ad (2): Nothing found locally, MISP may not have associated objects.
                    misp_uuid = vast.utils.stix.make_uuid(sightee.id)
                    data = self.misp.search(uuid=misp_uuid, controller="attributes")
                    logger.warning(data)

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


async def start(fabric: Fabric):
    await MISP(fabric).run()
