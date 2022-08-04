from .backbone import Backbone
from typing import Any, Callable


class InMemory(Backbone):
    """An in-memory backbone for intra-process communication."""

    def __init__(self):
        self.topics = {}

    async def publish(self, topic: str, data: Any):
        if topic in self.topics:
            for callback in self.topics[topic]:
                await callback(data)

    async def subscribe(self, topic: str, callback: Callable[[Any], None]):
        if topic in self.topics:
            self.topics[topic].append(callback)
        else:
            self.topics[topic] = [callback]
