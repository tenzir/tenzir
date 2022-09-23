---
sidebar_position: 0
---

# Design Goals

We designed Threat Bus with the following principles in mind:

- **Plugin-Based Architecture**: there are simply too many security tools out
  there to justify any other type of architecture. The plugin architecture
  reduces the quadratic complexity of interconnecting *N* tools with each
  another to simply supporting *N* independent tools. Security tools only need
  to how to connect with Threat Bus and are automatically integrated with every
  other tool that is connected to the bus.

- **Open Standard Formats**: Threat Bus uses the [STIX
  2.1](https://docs.oasis-open.org/cti/stix/v2.1/stix-v2.1.html) format for
  [indicators](https://docs.oasis-open.org/cti/stix/v2.1/cs02/stix-v2.1-cs02.html#_muftrcpnf89v)
  and
  [sightings](https://docs.oasis-open.org/cti/stix/v2.1/cs02/stix-v2.1-cs02.html#_a795guqsap3r).
  The goal is to comply with industry standards so that modern tools can
  integrate natively with Threat Bus with minimal message mapping overhead.

- **Modular Dependency Management**: plugins are packaged individually and can
  be installed independently of each other. This way, a Threat Bus host system
  can keep the list of required dependencies small and manageable.

- **Community Project**: Threat Bus is a free and open source project. We hope
  to come to a point where authors of awesome security tools can write the
  Threat Bus integration themselves. All contributions are welcome!

- **Inherited Scalability**: Conceptually, Threat Bus is a simple message
  passing engine. It inherits the scalability of its backbone. For example,
  using a distributed backbone, like
  [RabbitMQ](plugins/backbones/rabbitmq), allows for deploying
  multiple Threat Bus hosts in a scalable fashion.
