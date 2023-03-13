# ZeroMQ App Plugin

The ZeroMQ app plugin enables any application that can communicate via
[ZeroMQ](https://zeromq.org/) to subscribe to Threat Bus.

The plugin defines a simple [protocol](#management-protocol) for managing
(un)subscriptions from apps. In the following, we describe how to install and
configure the plugin, and how the management protocol enables building new
applications.

:::info Example Application&mdash;VAST - Threat Bus Connector
An example application that implements the management protocol is
[vast-threatbus](https://github.com/tenzir/threatbus/tree/master/apps/vast).
We use this wrapper because [VAST](https://github.com/tenzir/vast) cannot
communicate with Threat Bus natively. This Python application generates VAST
queries for indicators and reports the results back as sightings. It is
[documented](#application-example-vast---threat-bus-connector) below.
:::

## Installation

We recommend to use a virtual environment for the installation. Set it up
as follows:

```bash
python -m venv --system-site-packages venv
source venv/bin/activate
```

The plugin itself is published as [PyPI
package](https://pypi.org/project/threatbus-zmq/). All required dependencies
will be installed automatically when installing the plugin.

```bash
pip install threatbus-zmq
```

## Configuration

The plugin starts three listening ZeroMQ endpoints. The endpoint characteristics
for listening can be configured as follows:

```yaml
...
plugins:
  apps:
    zmq:
      host: "127.0.0.1"
      manage: 13370 # the port used for management messages
      pub: 13371 # the port used to publish messages to connected apps
      sub: 13372 # the port used to receive messages from connected apps
...
```

The `manage` endpoint handles (un)subscriptions, the `pub` endpoint
publishes new messages to all subscribers, and subscribers use the `sub`
endpoint to report back sightings. Check out the
[Communication Flow](#communication-flow) section
for details about these endpoints.

## Communication Flow

The ZeroMQ app plugin provides three endpoints for subscribers. Applications
initially only require the `manage` endpoint. The plugin sends the details about
the pub/sub ports to new subscribers during the subscription handshake.

Threat Bus (or rather, the ZeroMQ app plugin) creates a **unique queue and topic
for each new subscriber**. Upon successful registration, Threat Bus sends the
topic name to the subscriber. Subscribers then bind to the `pub` endpoint using
that topic. See the [management protocol](#management-protocol) section for
details.

Unique topics make snapshotting possible. Without unique topics, every
subscriber would potentially see the requested snapshot data of other
subscribers.

## Management Protocol

Subscriptions and unsubscriptions are referred to as `management` messages,
which are JSON formatted and exchanged via the `manage` ZMQ endpoint that the
plugin exposes.

The manage endpoint uses the
[ZeroMQ Request/Reply](https://learning-0mq-with-pyzmq.readthedocs.io/en/latest/pyzmq/patterns/client_server.html)
pattern for message exchange. That means, all messages get an immediate response
from the server. With each JSON reply, the zmq plugin sends a `status` field
that indicates the success of the requested operation.

### Subscription

To subscribe to Threat Bus via the zmq plugin, an app needs to send a JSON
struct as follows to the `manage` endpoint of the plugin:

```
{
  "action": "subscribe",
  "topic": <TOPIC>,       # either 'stix2/sighting' or 'stix2/indicator'
  "snapshot": <SNAPSHOT>  # number of days for a snapshot, 0 for no snapshot
}
```
In response, the app will either receive a `success` or `error` response.

- Error response:
  ```
  {
    "status": "error"
  }
  ```
- Success response:
  ```
  {
    "topic": <P2P_TOPIC>,
    "pub_port": "13371",
    "sub_port": "13372",
    "status": "success",
  }
  ```

  The `pub_port` and `sub_port` of the reply provide the port that an app should
  connect with to participate in the pub/sub message exchange. A connecting app
  can access these ports following the
  [ZeroMQ Pub/Sub](https://learning-0mq-with-pyzmq.readthedocs.io/en/latest/pyzmq/patterns/pubsub.html)
  pattern. The plugin will publish messages on the `pub_port` and accept
  messages on the `sub_port`.

  The `topic` field of the response contains a unique topic for the client. That
  topic **must** be used to receive messages. The unique topic is a 32
  characters wide random string and guarantees that other subscribers won't
  accidentally see traffic that should only be visible to the new client. See
  below for more details on [pub/sub via ZeroMQ](#pubsub-via-zeromq).

### Unsubscription

To unsubscribe, a connected app has to send a JSON struct to the `manage`
endpoint of the plugin, as follows:

```
{
  "action": "unsubscribe",
  "topic": <P2P_TOPIC>       # the 32-characters random topic that the app received during subscription handshake
}
```

In response, the app will either receive a `success` or `error` response.

- Error response:
  ```
  {
    "status": "error"
  }
  ```
- Success response:
  ```
  {
    "status": "success"
  }
  ```

### Heartbeats

The plugin supports synchronous heartbeats from subscribed apps. Heartbeats allow
Threat Bus and the connected apps to mutually ensure that their communication
partners are still alive.

Subscribed apps can send heartbeat messages with the following JSON format to
the `manage` endpoint of this plugin:

```
{
  "action": "heartbeat",
  "topic": <P2P_TOPIC>       # the 32-characters random topic that the app got during subscription handshake
}
```

As stated in the beginning of this section, the `manage` endpoint implements the
[ZeroMQ Request/Reply](https://learning-0mq-with-pyzmq.readthedocs.io/en/latest/pyzmq/patterns/client_server.html)
pattern. Threat Bus answers immediately to each heartbeat request with either a
`success` or `error` response.

- Error response:
  ```
  {
    "status": "error"
  }
  ```
- Success response:
  ```
  {
    "status": "success"
  }
  ```

An `error` response indicates that either Threat Bus has internal errors or that
it lost track of the app's subscription. Note: This only happens when Threat Bus
is restarted. Apps can then use that information to re-subscribe.

If Threat Bus does not answer a heartbeat message, it is either down or not
reachable (e.g., due to network issues). Plugins can use that information to try
again later.

### Pub/Sub via ZeroMQ

Once an app has subscribed to a certain Threat Bus topic using the `manage`
endpoint of the zmq plugin, it gets a unique, random `p2p_topic` (see
above). The `p2p_topic` differs from the subscribed Threat Bus topic in that the
zmq plugin uses only the `p2p_topic` to publish messages to subscribers.
Messages are either STIX-2 Indicators and Sightings, or are of the types
specified in
[`threatbus.data`](https://github.com/tenzir/threatbus/blob/master/threatbus/data.py),
i.e., `SnapshotRequest`, and `SnapshotEnvelope`.

ZeroMQ uses [prefix matching](https://zeromq.org/socket-api/#topics) for pub/sub
connections. The zmq plugin leverages this feature to indicate the type of
each sent message to the subscriber. Hence, an app can simply match the topic
suffix to determine the message type.

For example, all STIX-2 Indicators will always be sent on the topic
`p2p_topic + "indicator"`, all messages of the type
`threatbus.data.SnapshotRequest` on the topic `p2p_topic + "snapshotrequest"`,
and so forth.

## Application Example: VAST - Threat Bus Connector

Threat Bus is a publish-subscribe broker for security content. It expects that
applications register themselves at the bus. Since VAST cannot do so on its own,
VAST Threat Bus implements that functionality in the meantime.

VAST Threat Bus provides a thin layer around
[PyVAST](/docs/use/integrate/python), VAST's Python CLI bindings. It
facilitates message exchange between Threat Bus and a VAST instance,
transporting and converting STIX-2 Indicators and Sightings.

### Installation

You can either run the app directly by cloning the repository and invoking the
[Python file](https://github.com/tenzir/threatbus/blob/master/apps/vast) or you
can install it via `pip`. We recommend to install the app in a virtual
environment.

```bash
python -m venv --system-site-packages venv
source venv/bin/activate
python3 -m pip install vast-threatbus
```

### Usage

You can configure the app via a YAML configuration file. See the
[`config.yaml.example`](https://github.com/tenzir/threatbus/blob/master/apps/vast/config.yaml.example)
for an example config file that uses [fever
alertify](https://github.com/DCSO/fever) to transform sighting contexts before
they get printed to `STDOUT`. See the [Features](#features) section for details.

Start the application with a config file:

```sh
./vast_threatbus.py -c config.yaml
```

### STIX-2 Conversion

Threat Bus uses the [STIX-2 (version
2.1)](https://docs.oasis-open.org/cti/stix/v2.1/stix-v2.1.html) standard as data
format to transport
[Indicators](https://docs.oasis-open.org/cti/stix/v2.1/cs02/stix-v2.1-cs02.html#_muftrcpnf89v)
and
[Sightings](https://docs.oasis-open.org/cti/stix/v2.1/cs02/stix-v2.1-cs02.html#_a795guqsap3r).
The Threat Bus ZeroMQ app plugin simply exposes Indicators and Sightings in
STIX-2 format to subscribing apps, like VAST Threat Bus. Because VAST comes with
its own query language and its own internal IoC format for [live
matching](/docs/use/detect/match-threat-intel), VAST Threat Bus must
convert between the STIX-2 format and VAST formats.

The conversion currently only regards *point indicators*. A point indicator
consists of a single value and type, like a hostname or IP address.  VAST
Threat Bus converts STIX-2 Indicators both to valid VAST queries for retro
matching and/or to a JSON representation used by VAST for live matching.

Similarly, VAST Threat Bus converts both query results and matching results to
valid STIX-2 Sightings before sending them back to the ZeroMQ app plugin.

### Features

This section explains the most important features of VAST Threat Bus.

#### Live Matching

VAST's live matching works as a continuous query. VAST THreat Bus subscribes to
those continuous query results and reports all new IoC matches from VAST to
Threat Bus as `Sightings`. You can enable live matching in the config file by
setting `live_match: true`.

#### Retro Matching

VAST Threat Bus supports retro matching. You can enable it in the config file
by setting `retro_match: true`. This instructs the application to translate IoCs
from Threat Bus to normal VAST queries instead of feeding the IoCs to a live
matcher.

Each result from an IoC query is treated as `Sighting` of that IoC and reported
back to Threat Bus. You can limit the maximum amount of results returned from
VAST by setting the config option `retro_match_max_events` to a positive integer.

#### Sighting Context Transformation

You can configure VAST Threat Bus to invoke another program for parsing
Sighting `context` data via the config option `transform_context`.

If set, the app translates the `x_threatbus_sighting_context` field of a STIX-2
Sighting via the specified utility. For example, configure the app to pass the
context object to [DCSO/fever](https://github.com/DCSO/fever) `alertify`:

```yaml
...
transform_context: fever alertify --alert-prefix 'MY PREFIX' --extra-key my-ioc --ioc %ioc
...
```

The `x_threatbus_sighting_context` field can contain arbitrary data. For
example, retro matches from VAST contain the full query result in the context
field (like a Suricata EVE entry or a Zeek conn.log entry).

Note that the `cmd` string passed to `transform_context` is treated as
template string. The placeholder `%ioc` is replaced with the contents of the
actually matched IoC.

#### Custom Sinks for Sightings

VAST Threat Bus offers to send Sighting context to a configurable `sink`
_instead_ of reporting them back to Threat Bus. This can be configured via the
`sink` configuration parameter. The special placeholder `STDOUT` can be used to
print the Sighting context to `STDOUT`.

A custom sink is useful to forward `Sightings` to another process, like
`syslog`, or forward STDOUT via a UNIX pipe. Note that it may be desirable to
disable logging in that case.

Note that only the `x_threatbus_sighting_context` field of a STIX-2 Sighting is
printed, and not the object structure of the Sighting itself.
