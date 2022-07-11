from dynaconf import Dynaconf
from typing import Any

from backbones.backbone import Backbone


class VAST:
    def __init__(self, settings: Dynaconf, backbone: Backbone):
        self.apps = []
        self.settings = settings
        self.backbone = backbone

    async def publish(self, topic: str, data: Any):
        await self.backbone.publish(topic, data)

    async def subscribe(self, topic: str, callback):
        await self.backbone.subscribe(topic, callback)
