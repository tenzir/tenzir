from fabric import Fabric
import pymisp
import zmq
import zmq.asyncio
import json
import logging
import misp_stix_converter
from typing import Any

# The MISP app.
class MISP:
    def __init__(self, config: dict, fabric: Fabric):
        self.config = config
        self.fabric = fabric
        self.logger = logging.getLogger("MISP")
        self.logger.info("MISP App started")
        self.misp = pymisp.ExpandedPyMISP(
            url=self.config['api.host'],
            key=self.config['api.key'],
            ssl=False,
        )

    async def run(self):
        # Setup subscriptions.
        await self.fabric.subscribe("vast.sighting", self._on_sighting)
        # Hook into event feed via 0mq.
        socket = zmq.asyncio.Context().socket(zmq.SUB)
        zmq_host = self.config['zmq.host']
        zmq_port = self.config['zmq.port']
        socket.connect(f"tcp://{zmq_host}:{zmq_port}")
        socket.setsockopt(zmq.SUBSCRIBE, b"misp_json")
        try:
            while True:
                raw = await socket.recv()
                _, message = raw.decode("utf-8").split(" ", 1)
                try:
                    json_msg = json.loads(message)
                    self.logger.debug(json_msg)
                except Exception as e:
                    self.logger.error(f"failed to message {message}: {e}")
                    continue
                event = json_msg.get("Event", None)
                attribute = json_msg.get("Attribute", None)
                # Only consider events, not Attributes that ship within Events.
                if not event or attribute:
                    continue
                parser = misp_stix_converter.MISPtoSTIX21Parser()
                try:
                    parser.parse_misp_event(event)
                    self.logger.info(parser.bundle.serialize(pretty=True))
                    await self.fabric.publish("stix.bundle", parser.bundle)
                except Exception as e:
                    self.logger.error(f"failed to parse MISP event as STIX: {e}")
                    continue
        finally:
            socket.setsockopt(zmq.LINGER, 0)
            socket.close()

    async def _on_sighting(self, message: Any):
        self.logger.debug(f"got data: {message}")
        # TODO: exctract sighting
        #response = self.misp.add_sighting(...)
        #if not response or type(response) is dict and response.get("message", None):
        #   self.logger.error(f"failed to add sighting to MISP: '{sighting}' ({response})")
        #else:
        #    self.logger.info(f"reported sighting: {response}")


async def start(config: dict, fabric: Fabric):
    # Should by DynaConf or so eventually...
    config = {
        "api.host": "http://localhost:5000",
        "api.key": "demodemodemodemodemodemodemodemodemodemo",
        "zmq.host": "localhost",
        "zmq.port": 50000
        }
    misp = MISP(config, fabric)
    await misp.run()
