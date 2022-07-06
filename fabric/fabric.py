from backbone import Backbone
from typing import Callable, Coroutine


class Message:
    """An abstraction that will allow to switch between different payload types"""

    def __init__(self, b: bytes):
        self.data = b

    def from_bytes(b: bytes):
        return Message(b)

    def to_bytes(self) -> bytes:
        return self.data


class Fabric:
    """The VAST Fabric is what enables your apps to talk to each other"""

    def __init__(self, backbone: Backbone):
        self.backbone_impl = backbone

    async def publish(self, topic: str, data: Message):
        await self.backbone_impl.publish(topic, data.to_bytes())

    # callback: Callable[[Message], Coroutine[None]]
    async def subscribe(self, topic: str, callback):
        await self.backbone_impl.subscribe(
            topic, lambda m: callback(Message.from_bytes(m))
        )
