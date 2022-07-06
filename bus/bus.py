from backbone import Backbone, IN_MEMORY_BACKBONE_SINGLETON


class VASTBus:
    def __init__(self, backbone="memory", **kwargs):
        if backbone == "memory":
            self.backbone_impl: Backbone = IN_MEMORY_BACKBONE_SINGLETON
        else:
            raise NotImplementedError(f"Unknown backbone {backbone}")

    async def publish(self, channel: str, data: dict):
        await self.backbone_impl.publish(channel, data)

    async def subscribe(self, channel: str, callback):
        await self.backbone_impl.subscribe(channel, callback)
