# Threat Bus

:::caution
Threat Bus is in maintenance mode. We are no longer adding features, as we are
about to integrate the core concepts into a new version of VAST's Python
bindings.

We're happy to answer any question about the upcoming relaunch in our [community
chat](/discord).
:::

Threat Bus is a
[STIX](https://docs.oasis-open.org/cti/stix/v2.1/stix-v2.1.html)-based security
content fabric to connect security tools, such as network monitors like
[Zeek](https://zeek.org/), telemetry engines like
[VAST](https://github.com/tenzir/vast), or threat intelligence platforms (TIP)
like [OpenCTI](https://www.opencti.io) and
[MISP](https://www.misp-project.org/). Threat Bus wraps a tool's functions in a
publish-subscrbe fashion and connects it to a messaging backbone.

For example, Threat Bus turns a TIP into a feed of STIX Indicator objects that
can trigger action in other tools, such as installation into a blocklist or
executing a SIEM retro matching.

Threat Bus is a [plugin](threatbus/understand/plugins)-based application. Almost
all functionality is implemented in either
[backbone](threatbus/understand/plugins#backbone-plugins) or
[application](threatbus/understand/plugins#application-plugins) plugins. The
remaining logic of Threat Bus is responsible for launching and initializing all
installed plugins with the user-provided configuration. It provides some
rudimentary data structures for message exchange and subscription management, as
well as two callbacks for (un)subscribing to the bus.
