import asyncio
import fabric
import pymisp
import zmq
import zmq.asyncio
import json
import logging
import misp_stix_converter

# The MISP wrapper as seen from the fabric.
class MISP:
    def __init__(self, config: dict, fabric: fabric.Fabric):
        self.config = config
        self.fabric = fabric
        self.logger = logging.getLogger("MISP")
        self.subscriptions = ["stix.indicator"]
        self.logger.info("MISP App started")

    async def subscribe(self, topic: str):
        await self.fabric.subscribe(topic, self._callback)

    async def run(self):
        socket = zmq.asyncio.Context().socket(zmq.SUB)
        # TODO: configure according to self.config
        socket.connect("tcp://localhost:50000")
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
                # Only consider events, not Attributes that ship with Events
                if not event or attribute:
                    continue
                parser = misp_stix_converter.MISPtoSTIX21Parser()
                try:
                    parser.parse_misp_event(event)
                    self.logger.info(parser.bundle.serialize(pretty=True))
                    await self.fabric.publish("stix.bundle", fabric.Message(parser.bundle))
                except Exception as e:
                    self.logger.error(f"failed to parse MISP event as STIX: {e}")
                    continue
        finally:
            socket.setsockopt(zmq.LINGER, 0)
            socket.close()

    async def _callback(self, msg: fabric.Message):
        self.logger.debug(msg)


async def start(fabric: fabric.Fabric):
    config = {
        "api_host": "localhost",
        "api_port": 5000,
        "zmq_host": "localhost",
        "zmq_port": 50000
        }
    misp = MISP(config, fabric)
    await misp.run()
