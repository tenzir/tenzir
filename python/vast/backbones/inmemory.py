from collections import defaultdict

from vast import Backbone
from typing import Any, Callable


class InMemory(Backbone):
    """An in-memory backbone for intra-process communication."""

    def __init__(self):
        self.topics = defaultdict(list)

    async def publish(self, topic: str, data: Any):
        if topic in self.topics:
            for callback in self.topics[topic]:
                callback(data)

    async def subscribe(self, topic: str, callback: Callable[[Any], None]):
        self.topics[topic].append(callback)
