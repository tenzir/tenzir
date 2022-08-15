from abc import ABC, abstractmethod
from typing import Any, Callable


class Backbone(ABC):
    """The low-level medium that provides a topic-based publish-subscribe
    interface for event-based communication."""

    @abstractmethod
    async def publish(self, topic: str, data: Any):
        pass

    @abstractmethod
    async def subscribe(self, topic: str, callback: Callable[[Any], None]):
        pass
