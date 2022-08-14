from collections import defaultdict
from inspect import signature

from dynaconf import Dynaconf

from vast import Backbone
import vast.utils.logging

logger = vast.utils.logging.get(__name__)


class Context:
    """Object context to enable types to travel over the backbone of a
    fabric."""

    def __init__(self, type, name, encode=None, decode=None):
        self.type = type
        self.name = name
        self.encode = encode
        self.decode = decode

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

    def register(self, context: Context):
        """Register an object context."""
        logger.debug(f"registering object context {context}")
        self.registry[context.type].append(context)

    def context(self, type):
        """Retrieve the context for an object."""
        return self.registry[type]

    async def push(self, object):
        """Push a registered object into the fabric."""
        contexts = self.registry[type(object)]
        if not contexts:
            logger.error(f"no context registered for object {object}")
            return
        for ctx in contexts:
            if ctx.encode:
                await self.backbone.publish(topic(ctx), ctx.encode(object))
            else:
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
            if ctx.decode:
                f = lambda object: callback(ctx.decode(object))
                await self.backbone.subscribe(topic(ctx), f)
            else:
                await self.backbone.subscribe(topic(ctx), callback)
