---
sidebar_position: 2
---

# Plugins

Tenzir has a plugin system that makes it easy to hook into various places of
the data processing pipeline and add custom functionality in a safe and
sustainable way. A set of customization points allow anyone to add new
functionality that adds CLI commands, receives a copy of the input stream,
spawns queries, or implements integrations with third-party libraries.

There exist **dynamic plugins** that come in the form shared libraries, and
**static plugins** that are compiled into libtenzir or Tenzir itself:

![Plugins](plugins.excalidraw.svg)

Plugins do not only exist for extensions by third parties, but Tenzir also
implements core functionality through the plugin API. Such plugins compile as
static plugins. Because they are always built, we call them *builtins*.
