from abc import ABC, abstractmethod


class Backbone(ABC):
    @abstractmethod
    async def publish(self, topic: str, data: dict):
        pass

    @abstractmethod
    async def subscribe(self, topic: str, callback):
        pass


class InMemoryBackbone(Backbone):
    """A basic in-memory implementation of the backbone"""

    def __init__(self):
        self.topics = {}

    async def publish(self, topic: str, data: dict):
        print(f"Published on InMemoryBackbone")

    async def subscribe(self, topic: str, callback):
        print(f"Subscribed on InMemoryBackbone")
