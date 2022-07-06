import uuid
from abc import ABC, abstractmethod


class Backbone(ABC):
    @abstractmethod
    async def publish(self, channel: str, data: dict):
        pass

    @abstractmethod
    async def subscribe(self, channel: str, callback):
        pass


class InMemoryBackbone(Backbone):
    """A basic in-memory implementation of the backbone"""

    def __init__(self):
        self.topics = {}
        self.id = uuid.uuid4()

    async def publish(self, channel: str, data: dict):
        print(f"Published on InMemoryBackbone[{self.id}]")

    async def subscribe(self, channel: str, callback):
        print(f"Subscribed on InMemoryBackbone[{self.id}]")


# An instance of the in-memory backbone that can be shared across apps
IN_MEMORY_BACKBONE_SINGLETON = InMemoryBackbone()
