from collections import defaultdict
from typing import Any, Callable, TypeAlias

from dynaconf import Dynaconf

from vast import Backbone
import vast.utils.logging

logger = vast.utils.logging.get(__name__)

Converter: TypeAlias = Callable[[Any], Any]


def topic(name: str):
    """Creates a topic from a type name."""
    return f"/{name.replace('.', '/')}"


class Context:
    """Object context that offers type-specific metadata and functions."""

    def __init__(self, name):
        self._name = name
        self.transforms = defaultdict(list)

    def register(self, target: str, converter: Converter) -> None:
        """Registers a converter function to reach another type."""
        self.transforms[target].append(converter)

    def convert(self, object: Any) -> dict[str, Any]:
        """Transform a type into another type using a registered function."""
        result = {}
        for (name, funs) in self.transforms.items():
            result |= {name: fun(object) for fun in funs}
        return result

    @property
    def name(self):
        return self._name


class Fabric:
    """The high-level interface for object-based interaction over a specific
    backbone."""

    def __init__(self, config: Dynaconf, backbone: Backbone):
        self.config = config
        self.backbone = backbone
        self.registry = {}

    def register(self, source: str, sink: str, converter: Converter) -> None:
        """Register a converter between two types."""
        ctx = self.registry.get(source, None)
        if not ctx:
            logger.debug(f"creating new context for type {source}")
            ctx = Context(source)
            self.registry[source] = ctx
        logger.debug(f"registers converter: {source} -> {sink}")
        ctx.register(sink, converter)
        # TODO: update existing backbone subscriptions for newly registered
        # types.

    def convert(self, source: str, object: Any) -> None | Any | list[Any]:
        """Convert an object to another type."""
        if source not in self.registry:
            logger.error(f"no context for type {source}")
            return None
        match self.registry[source].convert(object):
            case [x]:
                return x
            case xs:
                return xs

    async def push(self, name: str, object: Any):
        """Push a registered object instance into the fabric."""
        # First push out the original object.
        await self.backbone.publish(topic(name), object)
        # Then push out all transformed objects.
        if name in self.registry:
            ctx = self.registry[name]
            for new_name, converted in ctx.convert(object).items():
                await self.backbone.publish(topic(new_name), converted)

    async def pull(self, name: str, callback):
        """Provide a callback for a registered object."""
        await self.backbone.subscribe(topic(name), callback)
