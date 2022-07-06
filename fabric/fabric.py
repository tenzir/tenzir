from backbone import Backbone
from collections.abc import Callable


class Message:
    def __init__(self, b: bytes):
        self.data = b

    def from_bytes(b: bytes):
        return Message(b)

    def to_bytes(self) -> bytes:
        return self.data


class Fabric:
    def __init__(self, backbone: Backbone):
        self.backbone_impl = backbone

    async def publish(self, topic: str, data: Message):
        await self.backbone_impl.publish(topic, data)

    # callback: Callable[[Message], None]
    async def subscribe(self, topic: str, callback):
        await self.backbone_impl.subscribe(topic, callback)
