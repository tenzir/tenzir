# MISP Plugin

The Threat Bus MISP plugin enables communication with the
[MISP](https://www.misp-project.org/) Open Source Threat Intelligence Platform.
The plugin handles all communication with MISP. It uses either
[ZeroMQ](https://zeromq.org/) or [Kafka](https://kafka.apache.org/) for
receiving new indicators data and reports back sightings to MISP via REST API
calls.

:::info MISP Module
The Threat Bus MISP plugin in its current form violates the pub/sub architecture
of Threat Bus. That is because the plugin subscribes a listener to MISP's
ZeroMQ / Kafka stream, rather than having MISP subscribe itself to Threat Bus.
This shortcoming will be addressed with a MISP module in the future.
:::

The plugin makes the following deployment assumptions:

1. Indicators of compromise (IoCs) are stored in MISP as *attributes*.
2. Either ZeroMQ or Kafka is enabled in MISP for attribute publishing.
3. The MISP REST API is enabled and an API key is available.

The plugin receives MISP attribute (IoC) updates from MISP, parses them to a
valid [STIX-2](https://oasis-open.github.io/cti-documentation/stix/intro.html)
Indicator, and publishes the parsed IoCs on the `stix2/indicator` topic. Other
apps that connect to Threat Bus, like [Zeek](zeek) or
[VAST](https://github.com/tenzir/vast) can consume those indicators by
subscribing to that Threat Bus topic.

Vice versa, the MISP plugin subscribes to the `stix2/sighting` topic and
converts STIX-2 Sightings to MISP sightings. It reports back these sightings to
the MISP platform using [PyMISP](https://github.com/MISP/PyMISP) to query the
MISP REST API.

## Installation

The plugin receives IoC updates from MISP either via ZeroMQ or Kafka. When using
Kafka, you have to install `librdkafka` for the host system that runs Threat
Bus. See the
[prerequisites](https://github.com/confluentinc/confluent-kafka-python#prerequisites)
section of the `confluent-kafka` Python client for more details.

Once the prerequisites are met, install the MISP plugin via pip. You can select
*optional dependencies* during installation for running either with Kafka or
ZeroMQ. Both options are available as follows:

```bash
pip install threatbus-misp[zmq]
pip install threatbus-misp[kafka]
```

If neither of these dependencies is installed (i.e., you installed
`threatbus-misp` without the `[...]` suffix for optional deps), the plugin
throws an error and exits immediately.

## Configuration

The plugin can either use ZeroMQ or Kafka to retrieve indicators from MISP. It
uses the MISP REST API to report back sightings of indicators.

ZeroMQ and Kafka are mutually exclusive, such that Threat Bus does not receive
all attribute updates twice. See below for an example configuration.

```yaml
...
plugins:
  misp:
    api:
      host: https://localhost
      ssl: false
      key: <MISP_API_KEY>
    filter: # filter are optional. you can omit the entire section.
      - orgs: # creator org IDs must be strings: https://github.com/MISP/PyMISP/blob/main/pymisp/data/schema.json
          - "1"
          - "25"
        tags:
          - "TLP:AMBER"
          - "TLP:RED"
        types: # MISP attribute types https://github.com/MISP/misp-objects/blob/main/schema_objects.json
          - ip-src
          - ip-dst
          - hostname
          - domain
          - url
      - orgs:
        - "2"
    zmq:
      host: localhost
      port: 50000
    #kafka:
    #  topics:
    #  - misp_attribute
    #  poll_interval: 1.0
    #  # All config entries are passed as-is to librdkafka
    #  # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    #  config:
    #    bootstrap.servers: "localhost:9092"
    #    group.id: "threatbus"
    #    auto.offset.reset: "earliest"
...
```

:::tip Kafka Fine-tuning

The MISP plugin forwards all settings from the `kafka.config` section of the
configuration file directly to the Kafka client. The used Python consumer is
[confluent-kafka.Consumer](https://docs.confluent.io/current/clients/confluent-kafka-python/#pythonclient-consumer).
For a list of all config options please see the official
[Kafka Client Configuration](https://docs.confluent.io/current/clients/confluent-kafka-python/#pythonclient-configuration)
docs.
:::

### Filter

The plugin can be configured with a list of filters. Every filter describes a
whitelist for MISP attributes (IoCs). The MISP plugin will only forward IoCs to
Threat Bus if the whitelisted properties are present.

A filter consists of three sub-whitelists for creator organizations, types, and
tags. To pass through the filter, an attribute must provide at least one of the
whitelisted properties of each of the whitelists. More precisely, entries of
each whitelist are linked by an `"or"`-function, the whitelists themselves are
linked by an `"and"`-function, as follows:
`(org_1 OR org_2) AND (type_1 OR type_2) AND (tag_1 OR tag_2)`.

The MISP plugin always assumes that the *absence of a whitelist means that
everything is whitelisted*. For example, when the entire `filter` section is
omitted from the config, then all attributes are forwarded and nothing is
filtered. More examples follow below.

#### Organizations

Organizations are whitelisted by their ID, which is a
[string](https://github.com/MISP/PyMISP/blob/main/pymisp/data/schema.json). Only
those MISP attributes that come from any of the whitelisted organizations will
be forwarded to Threat Bus.

#### Types

Types can be whitelisted by specifying MISP
[attribute types](https://github.com/MISP/misp-objects/blob/main/schema_objects.json).
Only those attributes that are instances of a whitelisted type will be forwarded
to Threat Bus.

#### Tags

MISP Attributes can be tagged with arbitrary strings. The tag whitelist respects
tag *names*. Only those attributes that have at least one of the whitelisted
tags will be forwarded to Threat Bus.

#### Examples:

This section provides some simple configuration examples to illustrate how
whitelist filtering works.

1. Forward all IoCs from the organizations `"1"` and `"25"`
```yaml
- orgs:
  - "1"
  - "25"
```
2. Forward only IoCs of the `domain`, `url`, or `uri` type, but only if they
  come from the organization `"1"` or `"25"`.
```yaml
- orgs:
  - "1"
  - "25"
- types:
  - domain
  - url
  - uri
```
3. Forward only IoCs that are tagged with `TLP:RED` or `TLP:AMBER`, but only of
  type `"src-ip"`:
```yaml
- tags:
  - "TLP:RED"
  - "TLP:AMBER"
- types:
  - src-ip
```

## Development Setup

The following guides describe how to set up a local, dockerized instance of
Kafka and how to setup a VirtualBox running MISP for developing.

### Dockerized Kafka

For a simple, working Kafka Docker setup use the
[single node example](https://github.com/confluentinc/cp-docker-images/blob/5.3.1-post/examples/kafka-single-node/docker-compose.yml)
from `confluentinc/cp-docker-images`.

Store the `docker-compose.yaml` and modify the Kafka environment variables such
that the Docker host (e.g., `172.17.0.1` on Linux) of your Docker machine is
advertised as Kafka listener:

```yaml
zookeeper:
  ...
kafka:
  ...
  environment:
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://172.17.0.1:9092   # <-- That is the IP of your Docker host
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
  ...
```

Check out [this article](https://rmoff.net/2018/08/02/kafka-listeners-explained/)
for details about Kafka listeners.

Then start the compose setup via `docker-compose up -d`.

To test the setup, use the `tests/utils/kafka_receiver.py` and
`tests/utils/kafka_sender.py` scripts in the Threat Bus
[repository](https://github.com/tenzir/threatbus).

### Local MISP using VirtualBox

This guide walks you through setting up MISP using a pre-configured VirtualBox
VM and then configuring MISP to export Attribute (IoC) updates to Threat Bus.

#### Installation via VirtualBox

Use the officially maintained
[Virtual Images](https://www.circl.lu/misp-images/_archive/) for MISP.
Download the latest `.ova` image file and load it in a VirtualBox client. Ensure
the following:

- The VM has enough working memory (e.g., 3 GiB of RAM)
- The VM exposes ports 8443 (web interface) and 50000 (ZMQ)
  - Use VirtualBox port-forwarding when NATting
  - Use VirtualBox bridge-mode & SSH into the VM using SSH port-forwarding

Here are the above steps as pure CLI instructions for running MISP in headless
mode (i.e., without a graphical VirtualBox interface).

```
curl -fL -o misp-2.4.138.ova https://www.circl.lu/misp-images/latest/MISP_v2.4.138@28ccbc9.ova
vboxmanage import misp-2.4.138.ova --vsys 0 --vmname misp --memory 3072 --cpus 1 --eula accept
vboxmanage modifyvm misp --nic1 nat
vboxmanage modifyvm misp --natpf1 "zmq,tcp,,50000,,50000"
vboxmanage list -l misp
```

You can then start and stop VM using the following commands:

```
vboxmanage startvm misp --type headless
vboxmanage controlvm misp poweroff
```

#### Configuration for usage with Threat Bus

For Threat Bus to receive attribute (IoC) updates from MISP, you must either
enable Kafka or ZMQ export in the MISP VM. If you chose to go with Kafka, you
need to install `librdkafka` first inside the VM, then make it known to PHP.

*Install Kafka inside VM*

```sh
ssh misp@<MISP_VM_IP> # enter your configured password to pop an interactive shell inside the VM
sudo apt-get update
sudo apt-get install software-properties-common
sudo apt-get install librdkafka-dev

# see https://misp.github.io/MISP/INSTALL.ubuntu1804/#misp-has-a-feature-for-publishing-events-to-kafka-to-enable-it-simply-run-the-following-commands
sudo pecl channel-update pecl.php.net
sudo pecl install rdkafka
echo "extension=rdkafka.so" | sudo tee /etc/php/7.2/mods-available/rdkafka.ini
sudo phpenmod rdkafka
sudo service apache2 restart
exit
```

Once Kafka is installed, you can go ahead and enable it in the MISP web-view.

*Enable Kafka export in the MISP web-view*

- Visit https://localhost:8443
- login with your configured credentials
- Go to `Administration` -> `Server Settings & Maintenance` -> `Plugin settings Tab`
- Set the following entries
  - `Plugin.Kafka_enable` -> `true`
  - `Plugin.Kafka_brokers` -> `172.17.0.1:9092`    <- In this example, 172.17.0.1 is the Docker host as configured in the Dockerized Kafka setup above, reachable from other Docker networks. The port is reachable when the Kafka Docker setup binds to it globally.
  - `Plugin.Kafka_attribute_notifications_enable` -> `true`
  - `Plugin.Kafka_attribute_notifications_topic` -> `misp_attribute` <- The topic goes into the threatbus `config.yaml`

You can use ZeroMQ to export IoCs from MISP as light weight alternative to
running Kafka. It does not require any extra installations, except enabling the
feature in the MISP web-view.

*Enable the ZMQ plugin in the MISP web-view*

- Visit https://localhost:8443
- login with your configured credentials
- Go to `Administration` -> `Server Settings & Maintenance` -> `Diagnostics Tab`
- Find the ZeroMQ plugin section and enable it
- Go to `Administration` -> `Server Settings & Maintenance` -> `Plugin settings Tab`
- Set the entry `Plugin.ZeroMQ_attribute_notifications_enable` to `true`
