from typing import Any, Callable

from dynaconf import Dynaconf
import stix2

from vast import Backbone
from vast.backbones.inmemory import InMemory


class Fabric:
    """The high-level interface for object-based interaction over a specific
    backbone."""

    # Backbones provide some mechanism for namespacing, so that other our
    # messages can co-exist with other workloads.
    TOPIC = "/vast/fabric"

    def __init__(self, config: Dynaconf, backbone: Backbone = InMemory()):
        self._config = config
        self._backbone = backbone

    @property
    def config(self):
        return self._config

    async def push(self, bundle: stix2.Bundle):
        """Push a registered object instance into the fabric."""
        await self._backbone.publish(self.TOPIC, bundle.serialize())

    async def pull(self, callback: Callable[[stix2.Bundle], Any]):
        """Provide a callback for a registered object."""
        marshall = lambda x: callback(stix2.parse(x, allow_custom=True))
        await self._backbone.subscribe(self.TOPIC, marshall)
