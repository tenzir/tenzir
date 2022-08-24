from typing import Any, Callable

from dynaconf import Dynaconf
import stix2

from vast import Backbone

class Fabric:
    """The high-level interface for object-based interaction over a specific
    backbone."""

    # Backbones provide some mechanism for namespacing, so that other our
    # messages can co-exist to other workloads.
    TOPIC = "/vast/fabric"

    def __init__(self, config: Dynaconf, backbone: Backbone):
        self.config = config
        self.backbone = backbone

    async def push(self, bundle: stix2.Bundle):
        """Push a registered object instance into the fabric."""
        await self.backbone.publish(self.TOPIC, bundle.serialize())

    async def pull(self, callback: Callable[[stix2.Bundle], Any]):
        """Provide a callback for a registered object."""
        marshall = lambda x: callback(stix2.parse(x, allow_custom=True))
        await self.backbone.subscribe(self.TOPIC, marshall)
