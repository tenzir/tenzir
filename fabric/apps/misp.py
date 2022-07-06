import asyncio
import fabric
import pymisp
import zmq
import json
import logging
import misp_stix_converter

# The MISP wrapper as seen from the fabric.
class MISP:
    def __init__(self, config: dict, fabric: fabric.Fabric):
        self.config = config
        self.fabric = fabric
        self.logger = logging.getLogger("MISP")
        self.logger.info("MISP App started")
        self.stix_parser = misp_stix_converter.MISPtoSTIX21Parser()
        self.subscriptions = ["stix.indicator"]

    async def subscribe(self, topic: str):
        await self.fabric.subscribe(topic, self._callback)

    async def run(self):
        socket = zmq.Context().socket(zmq.SUB)
        # TODO: configure according to self.config
        socket.connect("tcp://localhost:50000")
        socket.setsockopt(zmq.SUBSCRIBE, b"misp_json") # events
        #socket.setsockopt(zmq.SUBSCRIBE, b"misp_json_attribute")
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        while True:
            self.logger.debug("polling 0mq socket")
            socks = dict(poller.poll(timeout=1000))
            if socket not in socks or socks[socket] != zmq.POLLIN:
                continue
            raw = socket.recv()
            _, message = raw.decode("utf-8").split(" ", 1)
            try:
                json_msg = json.loads(message)
                self.logger.debug(json_msg)
            except Exception as e:
                self.logger.error(f"failed to message {message}: {e}")
                continue
            event = json_msg.get("Event", None)
            attribute = json_msg.get("Attribute", None)
            if not event or attribute:
                continue
            #attribute = json_msg.get("Attribute", None)
            stix = self.stix_parser.parse_misp_event(event)
            self.logger.info(stix)
            if isinstance(stix, dict):
                # We have a single object and can publish it, in case of
                # subscription.
                topic = f"stix.{stix['type']}"
                if topic in self.subscriptions:
                    await self.fabric.publish(topic, fabric.Message(stix))
            elif isinstance(stix, list):
                # We have a STIX Bundle and therefore multiple objects.
                self.logger.error("ignoring STIX Bundle")

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
