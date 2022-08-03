import json
from typing import Any

from dynaconf import Dynaconf
import misp_stix_converter
import pymisp
import zmq
import zmq.asyncio

from fabric import Fabric
import utils.logger

logger = utils.logger.create(__name__)

# The MISP app.
class MISP:
    def __init__(self, config: Dynaconf, fabric: Fabric):
        self.config = config
        self.fabric = fabric
        logger.info(f"connecting to REST API at {self.config.api.host}")
        try:
            self.misp = pymisp.ExpandedPyMISP(
                url=self.config.api.host,
                key=self.config.api.key,
                ssl=False)
        except pymisp.exceptions.PyMISPError as e:
            logger.error(f"failed to connect: {e}")

    async def run(self):
        # Setup subscriptions.
        await self.fabric.subscribe("vast.sighting", self._on_sighting)
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
                    await self.fabric.publish("stix.bundle", parser.bundle)
                except Exception as e:
                    logger.warning(f"failed to parse MISP event as STIX: {e}")
                    continue
        finally:
            logger.info(f"terminating MISP")
            self.misp = None
            socket.setsockopt(zmq.LINGER, 0)
            socket.close()

    async def _on_sighting(self, message: Any):
        logger.debug(f"got data: {message}")
        # TODO: exctract sighting
        #response = self.misp.add_sighting(...)
        #if not response or type(response) is dict and response.get("message", None):
        #   logger.error(f"failed to add sighting to MISP: '{sighting}' ({response})")
        #else:
        #    logger.info(f"reported sighting: {response}")


async def start(config: Dynaconf, fabric: Fabric):
    utils.logger.configure(config.logging, logger)
    misp = MISP(config.apps.misp, fabric)
    await misp.run()
