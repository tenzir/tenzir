from collections import defaultdict
from inspect import signature

from dynaconf import Dynaconf

from vast import Backbone
import vast.utils.logging

logger = vast.utils.logging.get(__name__)

class Context:
    """Object context to enable types to travel over the backbone of a
    fabric."""

    def __init__(self, type, name):
        self.type = type
        self.name = name


def topic(context: Context):
    """Creates a topic for an object context."""
    return f"/{context.name.replace('.', '/')}"


class Fabric:
    """The high-level interface for object-based interaction over a specific
    backbone."""

    def __init__(self, config: Dynaconf, backbone: Backbone):
        self.config = config
        self.backbone = backbone
        self.registry = defaultdict(list)

    def register(self, type, name: str):
        """Register an object context."""
        logger.debug(f"registering object {name}: {type}")

        self.registry[type].append(Context(type, name))

    async def push(self, object):
        """Push a registered object into the fabric."""
        contexts = self.registry[type(object)]
        if not contexts:
            logger.error(f"no context registered for object {object}")
            return
        for ctx in contexts:
            await self.backbone.publish(topic(ctx), object)

    async def pull(self, callback):
        """Provide a callback for a registered object."""
        sig = signature(callback)
        annotation = list(sig.parameters.values())[0].annotation
        contexts = self.registry[annotation]
        if not contexts:
            logger.error(f"no context registered for object {annotation}")
            return
        for ctx in contexts:
            logger.debug(f"subscribing to {topic(ctx)} for type {annotation}")
            await self.backbone.subscribe(topic(ctx), callback)
