# Zeek Plugin

The Threat Bus Zeek plugin enables communication with the
[Zeek](https://zeek.org/) network monitor. The plugin handles all communication
with Zeek via the "Zeek Messaging Library"
[Broker](https://github.com/zeek/broker).

The Zeek plugin converts IoCs from the STIX-2 Indicator format to Broker events
and forwards them to subscribed Zeek instances. The conversion happens on a
best-effort basis. When Zeek instances encounter an indicator match, they
send a Broker message to the Threat Bus Zeek plugin that converts it to a
valid STIX-2 Sighting.

:::caution Lossy Conversion
The [Zeek Intel Framework](https://docs.zeek.org/en/current/frameworks/intel.html)
only supports point-indicators, i.e., IoCs with only a single value like an IP
address or domain name. The STIX-2 standard can express more complex, compound
IoCs&mdash;these cannot be expressed with Zeek intelligence items.
:::

The plugin makes the following deployment assumptions:

1. Zeek instances that subscribe via the plugin's Broker endpoint must use the
  [threatbus.zeek](#threat-bus-zeek-script) script.
2. Subscribing Zeek instances have the
   [Intelligence Framework](https://docs.zeek.org/en/current/frameworks/intel.html)
   loaded and enabled so they can match IoCs.

## Installation

The plugin uses the
[Broker python bindings](https://docs.zeek.org/projects/broker/en/stable/python.html)
for native interaction with Zeek. Broker and the Python bindings need to be
installed on the Threat Bus host system to use this plugin. Please consult
[the official Broker documentation](https://docs.zeek.org/projects/broker/en/current/python.html#installation-in-a-virtual-environment)
for installation instructions.

:::warning Zeek/Broker Compatibility
If you install Zeek and Broker manually, you must ensure
that the installed versions are compatible with each other. Version
incompatibilities can lead to silent errors.

Check the [Broker releases](https://github.com/zeek/broker/releases) page on
GitHub for compatibility with Zeek.
:::

Once the prerequisites are met, install the Zeek plugin via pip.

```bash
pip install threatbus-zeek
```

## Configuration

The plugin starts a listening Broker endpoint. The endpoint characteristics for
listening can be configure as follows.

```yaml
...
plugins:
  apps:
    zeek:
      host: "127.0.0.1"
      port: 47761
      module_namespace: Tenzir
...
```

The last parameter `module_namespace: Tenzir` is required for Zeek's messaging
library `Broker`. This namespace is set in the
[threatbus.zeek](#threat-bus-zeek-script) script.

## Threat Bus Zeek Script

Threat Bus is a pub/sub broker for security content. Applications like Zeek have
to register themselves at the bus. Zeek cannot communicate with Threat Bus out
of the box, so we provide a Zeek script
[`threatbus.zeek`](https://github.com/tenzir/threatbus/blob/master/apps/zeek/threatbus.zeek)
in the Threat Bus GitHub repository.

The script equips Zeek with the capability to communicate with Threat Bus, including
the un/subscription management and the conversion logic between Broker events
and indicators & sightings. The script installs an event hook in Zeek that
triggers on intelligence matches. Should these matches be related to IoCs that
originate from Threat Bus, a proper sighting is generated and sent back.

Users can configure the script via CLI options. See the following list of all
available options:

Option Name           | Default Value       | Explanation
----------------------|---------------------|-------------
broker_host           | "127.0.0.1"         | IP address of the Threat Bus host running the Zeek plugin. For the plugin's configuration see the Threat Bus `config.yaml` file.
broker_port           | 47761/tcp           | Port of the Zeek plugin's Broker endpoint. For the plugin's configuration see the Threat Bus `config.yaml` file.
report_sightings      | T                   | Toggle to report back sightings to Threat Bus.
noisy_intel_threshold | 100                 | The number of matches per second an indicator must exceed before we report it as "noisy".
log_operations        | T                   | Toggle to enable/disable logging.
intel_topic           | "stix2/indicator"   | The Threat Bus topic to subscribe for IoC updates.
sighting_topic        | "stix2/sighting"    | The Threat Bus topic to report sightings to.
management_topic      | "threatbus/manage"  | A Broker topic, used for internal negotiations between Zeek instances and the Threat Bus Zeek plugin.
snapshot_intel        | 0 sec               | User-defined interval to request a snapshot of historic indicators.

To set options of the Zeek script via CLI invoke it as follows:

```bash
zeek -i <INTERFACE> -C ./apps/zeek/threatbus.zeek "Tenzir::snapshot_intel=30 days"
```
