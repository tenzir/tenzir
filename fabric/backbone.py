from abc import ABC, abstractmethod
from typing import Callable
import asyncio


class Backbone(ABC):
    @abstractmethod
    async def publish(self, topic: str, data: bytes):
        pass

    @abstractmethod
    async def subscribe(self, topic: str, callback: Callable[[bytes], None]):
        pass


class InMemoryBackbone(Backbone):
    """A basic in-memory implementation of the backbone"""

    def __init__(self):
        self.topics = {}

    async def publish(self, topic: str, data: bytes):
        if topic in self.topics:
            for callback in self.topics[topic]:
                await callback(data)

    async def subscribe(self, topic: str, callback: Callable[[bytes], None]):
        if topic in self.topics:
            self.topics[topic].push(callback)
        else:
            self.topics[topic] = [callback]
