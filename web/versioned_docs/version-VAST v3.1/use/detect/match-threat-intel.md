---
sidebar_position: 0
---

# Match Threat Intelligence

import CommercialPlugin from '@site/presets/CommercialPlugin.md';

<CommercialPlugin />

:::info Terminology: Threat Intelligence
Threat intelligence is security content that describes threats from various
perspectives. Practitioners typically distinguish strategic, operational, and
tactical threat intelligence. We focus on the tactical data that decomposes into
*observables* as singular data points, or more specifically *indicators of
compromise (IoCs)* that reflect malicious activity.
:::

VAST can live-match threat intelligence against the incoming stream of events,
producing an alert feed of sightings. This feature fits into the bigger theme
of a [unified detection](../../about/use-cases/unified-detection.md) strategy
with a security-content-driven workflow.

VAST features *matchers* that check whether specific field values exist in
dynamic set of indicators. A successful match emits a *sighting* as output.
This functionality resembles [Suricata's datasets][datasets] or [Zeek's intel
framework][intel-framework], but generalized to all security telemetry. Key
matcher features include:

[datasets]: https://suricata.readthedocs.io/en/latest/rules/datasets.html
[intel-framework]: https://docs.zeek.org/en/master/frameworks/intel.html

- **Exact & Fuzzy Mode**: controllable memory usage through multiple storage
  backends, such as hash tables, Bloom filters, and Cuckoo filters.

- **Surgical Target Locking**: fine-grained configuration options to dispatch
  matchers to fields in the data, fully leveraging VAST's type system.

- **Composable Sighting Streams**: mix-and-match sighting streams to combine
  the results of matchers, e.g., fuse TLP:RED and inhouse indicators in one
  stream and OSINT and TLP:WHITE in a another one.

- **Full Control**: flexible controls to add/remove indicators, perform
  bulk-imports, and save/restore binary matcher state.

Working with matchers involves three separate steps:

1. [Start a matcher](#start-matchers)
2. [Add](#add-indicators)/[remove](#remove-indicators) indicators
3. [Attach to the matcher](#attach-to-matchers) to consume sightings

VAST uniquely identifies matchers by their name, either as specified in the YAML
configuration or on the command line. Whenever interacting with a matcher, you
need to pass the name as argument to all operations. The general pattern looks
as follows:

```bash
vast matcher <command> [options] <name>
```

VAST also supports executing operations on multiple matchers at once, e.g., to
a add an indicator to a many matchers. To this end, simply use a comma-separated
list for the positional `name` argument, e.g., `vast matcher add a,b,c ...` to
act on matchers `a`, `b`, and `c`.

:::note Requirements
To use matchers, make sure that your VAST distribution has the `matcher` plugin
available, e.g., by checking the output of `vast version`:

```bash
vast -q --plugins=all version | jq .plugins.matcher
```
:::

## Start Matchers

There exist two methods to start matchers:

1. Server-side: configure them in the `vast.yaml` configuration
2. Client-side: invoke `vast matcher start` on the command line

Method (1) produces *persistent* matchers that survive restarts and flush their
state periodically; (2) produces *ephemeral* matchers, which are functionally
equivalent but require manual state management if persistence is desired.

A matcher operates in a specific *mode*. Please consult the section [matcher
modes](#understand-matcher-modes) below to understand the trade-offs.

### Server-side

The configuration key `plugins.matcher` contains the configuration for
*persistent* matchers, i.e., those that survice restarts and get periodically
persisted.

Here is an example configuration snippet:

```yaml
plugins:
  matcher:
    # The amount of time to wait before triggering a write to disk for matchers
    # "dirty" matchers, i.e., those that have been modified since the last
    # write.
    persistence-interval: 30 mins
    # VAST automatically starts all matchers configured in this section.
    matchers:
      # An exact matcher that operates on fields.
      hostnames:
        mode: exact
        match-fields:
          - net.domain
          - net.hostname
      # A Cuckoo matcher that operates on all fields of type IP.
      ips:
        mode: cuckoo
        match-types:
          - ip
      # A DCSO bloom matcher that operates on all fields of type string
      iocs:
        mode: dcso-bloom
        match-types:
          - string
        capacity: 1000000
        false-positive-probability: 0.001
```

Adding a matcher means adding a new entry under the key `matchers`.

The matcher-global option `persistence-interval` controls how fast a persist
operation takes place after a state mutation. Regardless of the configured
value, VAST persists all matchers with pending modifications on shutdown.

### Client-side

When deploying matchers, editing the server-side configuration can be unwieldy
and result int undesired blind spots, because they require a restart of the
server for the configuration changes to take effect. This is why VAST also
supports spawning *ephemeral* matchers via the CLI. Ephemeral matchers behave
exactly like persistent matchers, with the only difference that the VAST server
doesn't manage their state. However, it is still possible to [manually save/load
the matcher state](#manage-matcher-state).

To start an ephemeral matcher, use `vast matcher start`. The command line
options are identical to the YAML keys. For example, to spawn the `iocs`
matcher configured above as ephemeral matcher, use this command:

```bash
vast matcher start \
  --mode=dcso-bloom \
  --capacity=1000000 \
  --false-positive-probability=0.001 \
  --match-types=string \
  iocs
```

## List Matchers

To show the running matchers, use `vast matcher list`. Example output may look
like this:

```
hostnames (disabled with 0 clients)
strings (disabled with 0 clients)
ips (disabled with 0 clients)
```

Matchers are *enabled* when one or more clients are attached. See the next
section on how to attach to a matcher.

## Attach to Matchers

Unless you attach to a matcher, it will not hook into the ingress event stream
in order to conserve resources. A matcher is *enabled* if it has at least one
connected client.

To attach to a matcher, you need to specified output format and the matcher
name:

```bash
vast matcher attach csv hostnames
```

The process will block and print all sightings in CSV format on standard
output, until it receives a termination signal, e.g., by pressing CTRL+C or
sending it SIGINT.

In the common case, you don't want to repeat this for every matcher. To attach
to multiple matchers with a single client, provide their names as
list:

```bash
vast matcher attach json hostnames,ips,iocs
```

You will now receive sightings from all matchers in JSON format. There is no
ordering guarantee on the sighting output, as VAST fuses the sighting stream
asynchronously to deliver optimal latency.

## Add Indicators

There exist two methods to populate matchers with content:

1. Add a single indicator
2. Bulk-import a set of indicators

### One-shot Import

Adding a single indicator involves passing it on the command line:

```bash
vast matcher add <matcher> <value> [context]
```

For example, to add and IP address along with an opaque identifier to the
matcher `ips`, use:

```bash
vast matcher add ips 6.6.6.6 opaque-id-42
```

The context value `opaque-id-42` will show in in all sightings for this
indicator, e.g., to associate it with an external unique ID.

:::caution Context Usability
The context argument is only supported by the *exact* matcher. Probabilistic
matchers cannot store the extra context data. Please consult the section on
[matcher modes](#understand-matcher-modes) to understand the inherent trade-offs.
:::

### Bulk Import

Adding large sets of indicators using `vast matcher add` does not scale,
because the overhead of establishing a connection to the server dwarfs the time
it takes to implant the indicator into the corresponding data structure. To
import large sets of indicators in bulk, use the `vast matcher import` command.

The `vast matcher import` command mirrors the interface of the `vast import`
command. Instead of importing events into the database, it imports events
containing *indicators* and forwards them to selected matchers. Let's take a
look at an example incovation using the [Pulsedive threat intelligence
feed](https://pulsedive.com/about/feed):

```bash
# Pulsedive feed without retired indicators and restricted to ip and ipv6 IoCs.
feed_url='https://pulsedive.com/premium/?key=&header=true&fields=id,type,risk,threats,feeds,usersubmissions,riskfactors,reference&types=ip,ipv6&risk=unknown,none,low,medium,high,critical&period=all&retired=false'

# Ingest the feed into the matcher 'ips' we created above.
curl -sSL "$feed_url" |
  vast matcher import -t pulsedive csv ips
```

The `curl` command downloads a CSV and dumps it to STDOUT. The `vast matcher
import` command reads CSV content by specifying `csv` as first positional
argument. We are also telling VAST via `-t pulsedive` that the data matches the
`pulsedive` type (specified in the bundled `pulsedive.schema`). After parsing,
VAST forwards the parsed indicators to the matcher `ips`.

An additional [concepts
definition](../../understand/data-model/taxonomies.md#concepts) for the
`matcher.indicator.value` and `matcher.indicator.context` fields for the
`pulsedive` type lets the command know which fields to treat as *value* and
optional *context*.

The above example used a pre-filtered list from Pulsedive. However, import
filter expressions allow for doing the filtering on the fly using VAST's
regular import filter expressions like this:

```bash
# The full Pulsedive feed.
feed_url='https://pulsedive.com/premium/?key=&header=true&fields=id,type,risk,threats,feeds,usersubmissions,riskfactors,reference&types=ip,ipv6,domain,url&risk=unknown,none,low,medium,high,critical&period=all&retired=true'

# Ingest the feed into the matcher 'ips', but skip all retired indicators.
curl -sSL "${feed_url}" |
  vast matcher import -t pulsedive csv ips \
    'risk != /:retired/ && type == /ip.*/
```

The matcher plugin conveniently ships with a Pulsedive schema and concept
definitions for use with the matcher plugin in
`<sysconfdir>/share/vast/plugins/matcher/schema`.

## Delete Indicators

The `remove` command is the dual to `add`: it removes a single indicator value.
For example, to remove `6.6.6.6` from the matcher `ips`, invoke:

```bash
vast matcher remove ips 6.6.6.6
```

:::caution Context Usability
Not all matchers support deletion of indicators. Please consult the section on
[matcher modes](#understand-matcher-modes) to understand the inherent
trade-offs. :::

Bulk deletion is currently not possible, but you can [manage the matcher state
manually](#manage-matcher-state), e.g., to externally constructed reload Bloom
filters.

## Manage Matcher State

To simplify managing of large sets of indicators for operators, VAST supports
client-side modification of the underlying raw matcher state.

For example, this allows you to compile a Bloom filter containing several
millions of indicators in your threat intelligence platform and synchronize the
content for matching in VAST. Another use case involves dumping matcher state
to replicate the matcher at another VAST instance.

### Save/load state at the client

To show the state of a specific matcher, use the `matcher save` command:

```bash
vast matcher save ips > ips.state
```

The command writes the binary state of the matcher `ips` to standard output,
expecting users to redirect it according to their needs. The state is portable,
and you can copy it over to other machines as well.

To replace the state of a running matcher, use the `matcher load` command:

```bash
vast matcher load ips < ips.state
```

The command reads the binary state from standard input.

:::tip Migrating Matchers
You can also combine `save` and `load` to migrate the state of one matcher,
e.g., to perform a modification that you want to reverse later on, or to "fork"
a matcher. To migrate matcher `foo` to matcher `bar`, use:

```bash
vast matcher save foo | vast matcher load bar
```
:::

## Understand Matcher Modes

Fundamentally, a matcher maintains set of indicators. The *mode* controls how
the matcher stores the indicator data. The table below gives a quick summary
about the trade-offs when choosing a mode:

| Mode        | Name         | Add | Delete | Context | Space                      |
|-------------|--------------| ----|--------|---------|----------------------------|
| Exact       | `exact`      | ✔︎   | ✔︎      |  ✔︎      | *O(n)*                     |
| Cuckoo      | `cuckoo`     | ✔︎   | ✔︎      |  ✘      | *O((log(1/p) + 2) / load)* |
| DCSO bloom  | `dcso-bloom` | ✔︎   | ✘      |  ✘      | *O(1.44 log(1/p))*         |

### Exact

The `exact` mode maintains a key-value mapping in the form of a hash map using
[robin hood hashing][robin-hood-hashing]. Every key in the table represents the
indicator item. The value is optional context that can be chosen freely.

The exact matcher supports all operations, at the cost of growing linearly with
the number of indicators.

### Cuckoo

The `cuckoo` mode summarizes the set of indicators in a [Cuckoo
filter][cuckoo-filter].

Compared to Bloom filters, Cuckoo filters have the following advantages:
- Support for deleting previously inserted elements
- Better false-positive probability as the filter load increases
- Smaller memory footprint for false-positive probabilities less than 3%.
- *O(1)* vs *O(k)* operations, where *k* is the number of hash functions in the
  Bloom filter

:::caution Deleting Elements
The delete operation comes with a caveat: it is only well-defined if the
to-be-deleted item has been previously added. Otherwise the filter enters an
undefined state and can produce false negatives in addition to false positives.
VAST cannot enforce this pre-condition, so you must tread carefully when using
it.
:::

The Cuckoo filter is currently not parameterizable. The size is always 128 MiB.
In the future, we will offer the same tuning knobs as the DCSO Bloom filter
below.

### DCSO Bloom

The `dcso-bloom` mode stores the indicators in a [Bloom filter][bloom-filter].

The two tuning knobs are *capacity* (maximum number of items in the filter) and
*false-positive probability* (chance of reporting an indicator not in the
filter). The two parameters dictate the space usage. Please consult Thomas
Hurst's [Bloom Filter Calculator](https://hur.st/bloomfilter/) for finding the
optimal configuration for your use case.

The Bloom ftiler is complete C++ rebuild of DCSO's Bloom filter library
[`bloom`](https://github.com/dcso/bloom). VAST's implementation is
binary-compatible and uses the exact same method for FNV1 hashing and parameter
calculation, making it a drop-in replacement for `bloom` users.

##### Constructing a bloom matcher

To construct a `dcso-bloom` matcher, use `matcher start`. The additional
parameters `--false-positive-probability` (`-n`) and `--capacity` (`-n`) allow
for controlling the underlying Bloom filter:

```bash
vast matcher start --mode=dcso-bloom -p 0.1 -n 100 --match-fields=net.domain ns
vast matcher add ns 1.1.1.1
vast matcher add ns 8.8.8.8
```

#### Importing bloom-generated binary filters

In addition to controlling matcher content using the `matcher add` and `matcher
import` commands, you can provide a binary Bloom filter created by the Go
utility `bloom`:

```bash
bloom create -p 0.1 -n 100 ns.bloom
echo 1.1.1.1,8.8.8.8 | bloom -s insert ns.bloom
```

Use `bloom show ns.bloom` to display a few statistics about the filter, such as
the number of elements, false-positive probability, number of hash functions,
and bits used.

Finally, we hand the Bloom filter over to VAST and associate it with the
matcher called `ns`:

```bash
vast matcher load dns < ns.bloom
```

See the section on [matcher state management](#manage-matcher-state) for
a more detailed discussion on loading binary state into the matcher.

## Examples

This section includes real-world examples to illustrate how the matcher works
in practice.

### IP Blocklists

IP blocklists make up for a large share of low-level IoCs, often to represent
attacker infrastructure, such as C2 servers.

The [Feodo Tracker](https://feodotracker.abuse.ch/) from
[abuse.ch](https://abuse.ch) represents one such blocklist that gets updated
every 5 minutes. Let's take a look:

```bash
head -n 15 ipblocklist.csv
################################################################
# abuse.ch Feodo Tracker Botnet C2 IP Blocklist (CSV)          #
# Last updated: 2021-08-17 15:00:42 UTC                        #
#                                                              #
# Terms Of Use: https://feodotracker.abuse.ch/blocklist/       #
# For questions please contact feodotracker [at] abuse.ch      #
################################################################
#
"first_seen_utc","dst_ip","dst_port","c2_status","last_online","malware"
"2021-01-17 07:30:05","67.213.75.205","443","offline","2021-08-18","Dridex"
"2021-01-17 07:44:46","51.178.161.32","4643","online","2021-08-18","Dridex"
"2021-01-17 07:44:50","162.144.127.197","3786","online","2021-08-18","Dridex"
"2021-01-17 07:45:55","111.230.104.169","3388","online","2021-08-18","Dridex"
"2021-01-17 07:45:58","217.79.184.243","33443","online","2021-08-18","Dridex"
"2021-01-17 07:47:59","46.101.90.205","4643","online","2021-08-18","Dridex"
```

Before VAST can read this data, we need to tell VAST what type to use for it. We
write an `abuse` [module](../../understand/data-model/modules.md) for this:

```yaml
module: abuse
types:
  feodo.blocklist:
    record:
      - first_seen_utc: time
      - dst_ip: ip
      - dst_port: port
      - c2_status:
          enum:
            - online
            - offline
      - last_online: time
      - malware: string
```

In addition, you need to tell VAST what fields have the indicator data,
consisting of value and an optional context. To this end, you need to provide a
[concept](../../understand/data-model/taxonomies.md#concepts) definition:

```yaml
concepts:
  matcher.indicator.value:
    fields:
      - abuse.feodo.blocklist.dst_ip
  matcher.indicator.context:
    fields:
      - abuse.feodo.blocklist.malware
```

Now we can translate the blocklist into a format that VAST can read, e.g., CSV
or JSON. In the example below, we simply add a header to the plain text file to
create a valid CSV. (Feodo also provides a CSV download, but we want to
illustrate how you can easily perform a translation.)

```bash
curl -sSL https://feodotracker.abuse.ch/downloads/ipblocklist.csv |
  tr -d '\015' |
  grep -v '^#' |
  vast matcher import -t feodo.blocklist csv ips
```

We throw in a `tr -d '\015'` to convert DOS linebreaks to UNIX and strip `#`
comments via `grep -v`.

The live matcher `ips` is now armed with the Feodo blocklist and matches it on
all IP addresses.

[robin-hood-hashing]: https://en.wikipedia.org/wiki/Hash_table#Robin_Hood_hashing
[cuckoo-filter]: https://en.wikipedia.org/wiki/Cuckoo_filter
[bloom-filter]: https://en.wikipedia.org/wiki/Bloom_filter
