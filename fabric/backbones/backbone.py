from abc import ABC, abstractmethod
from typing import Any, Callable


class Backbone(ABC):
    @abstractmethod
    async def publish(self, topic: str, data: Any):
        pass

    @abstractmethod
    async def subscribe(self, topic: str, callback: Callable[[Any], None]):
        pass
