# Get Started

<!-- Keep in sync with project README at https://github.com/tenzir/tenzir -->

:::info What is Tenzir?
Tenzir is a distributed pipeline platform for processing and storing security
event data in a pipeline dataflow model.
:::

Dive right in with an interactive tour at [tenzir.com](https://tenzir.com) and
sign up for a free account, or continue below with the open source edition and
command line examples.

### Install Tenzir

Select your platform to download and install Tenzir.

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
  <TabItem value="universal" label="All Platforms" default>

  Let our installer take care of getting started:

  ```bash
  curl https://get.tenzir.com/ | sh
  ```

  </TabItem>
  <TabItem value="linux" label="Linux">
  
  Download our static binary as Debian package or tarball for other Linux
  distributions:

  <div class="padding--sm">
    <a class="button button--md button--info margin--md" href="https://github.com/tenzir/tenzir/releases/latest/download/tenzir-linux-static.deb">deb</a>
    <a class="button button--md button--primary margin--md" href="https://github.com/tenzir/tenzir/releases/latest/download/tenzir-linux-static.tar.gz">tar.gz</a>
  </div>

  Install the Debian package via `dpkg`:

  ```bash
  dpdk -i tenzir-linux-static.deb
  ```

  Alternatively, unpack the tarball into `/opt/tenzir`:

  ```bash
  tar xzf tenzir-linux-static.tar.gz -C /
  ```
  </TabItem>
  <TabItem value="macos" label="macOS">

  Please use Docker [with
  Rosetta](https://levelup.gitconnected.com/docker-on-apple-silicon-mac-how-to-run-x86-containers-with-rosetta-2-4a679913a0d5)
  until we offer a native package.

  </TabItem>
  <TabItem value="docker" label="Docker">

  Pull the open source image:

  ```bash
  docker pull tenzir/tenzir
  ```

  When using Docker, replace `tenzir` with `docker run -it tenzir/tenzir` in the
  examples below.

  </TabItem>
</Tabs>

### Download sample data

The below examples use this dataset. Download to follow along or just keep
reading.

```bash
# Suricata EVE JSON logs (123 MB)
curl -L -O https://storage.googleapis.com/tenzir-datasets/M57/suricata.tar.gz
tar xzvf suricata.tar.gz
# Zeek TSV logs (43 MB)
curl -L -O https://storage.googleapis.com/tenzir-datasets/M57/zeek.tar.gz
tar xzvf zeek.tar.gz
```

### Run pipelines

The `tenzir` exectuable runs a pipeline.

Start with the [`version`](operators/sources/version.md) source operator and
pipe to the [`write`](operators/sinks/write.md) sink operator:

```bash
tenzir 'version | write json' 
```

```json
{"version": "v3.1.0-377-ga790da3049-dirty", "plugins": [{"name": "parquet", "version": "bundled"}, {"name": "pcap", "version": "bundled"}, {"name": "sigma", "version": "bundled"}, {"name": "web", "version": "bundled"}]}
```

Get the top 3 schemas of the Zeek schemas as tab-separated values
([`tsv`](formats/tsv.md)):

```bash
cat Zeek/*.log | tenzir '
  read zeek-tsv 
  | measure 
  | summarize events=sum(events) by schema 
  | sort events desc 
  | head 3 
  | write tsv
  '
```

<details>
<summary>Output</summary>

```
schema	events
zeek.conn	583838
zeek.ssl	42389
zeek.files	21922
```

</details>

Get the top 5 Zeek notices from `notice.log` as JSON:

```bash
tenzir '
  from file Zeek/notice.log
  | read zeek-tsv 
  | where #schema == "zeek.notice"
  | summarize n=count(msg) by msg
  | sort n desc 
  | head 3 
  | write json
  '
```

<details>
<summary>Output</summary>

```json
{"msg": "SSL certificate validation failed with (certificate has expired)", "n": 2201}
{"msg": "SSL certificate validation failed with (unable to get local issuer certificate)", "n": 1600}
{"msg": "SSL certificate validation failed with (self signed certificate)", "n": 603}
{"msg": "Detected SMB::FILE_WRITE to admin file share '\\\\10.5.26.4\\C$\\WINDOWS\\h48l10jxplwhq9eowyecjmwg0nxwu72zblns1l3v3c6uu6p6069r4c4c5yjwv_e7.exe'", "n": 339}
{"msg": "SSL certificate validation failed with (certificate is not yet valid)", "n": 324}
```

</details>

### Spawn a node

Use the `tenzir-node` executable to a node that manages pipelines and storage.

<Tabs>
  <TabItem value="binary" label="Binary" default>

  ```bash
  tenzir-node
  ```

  ```
  [12:43:22.789] node (v3.1.0-377-ga790da3049-dirty) is listening on 127.0.0.1:5158
  ```

  </TabItem>
  <TabItem value="docker" label="Docker">

  Expose the port of the listening node and provide a directory for storage:

  ```bash
  mkdir storage
  docker run -dt -p 5158:5158 -v storage:/var/lib/tenzir tenzir/tenzir --entry-point=tenzir-node
  ```

  </TabItem>
</Tabs>

### Import data into a node

End a pipeline with the [`import`](operators/sinks/import.md) operator to ingest
data into a node:

```bash
tar xOzf zeek.tar.gz | tenzir 'read zeek | import'
```

Filter the input with [`where`](operators/transformations/where.md) to select a
subset of events:

```bash
tar xOzf suricata.tar.gz | tenzir 'read zeek | where #schema == "suricata.alert" | import'
```

### Export data from a node

Start a pipeline with the [`export`](operators/sources/export.md) operator to
initiate a datastream from stored data at a node.

Get a "taste" of one event per schema:

```bash
tenzir 'export | taste 1'
```

<details>
<summary>Output</summary>

TODO

</details>

As above, get the top 3 schemas of the Zeek schemas, but this time start the
pipeline over the historical data at the running node:

```bash
tenzir '
  export
  | measure 
  | summarize events=sum(events) by schema 
  | sort events desc 
  | head 3 
  | write tsv
  '
```

<details>
<summary>Output</summary>

```
schema	events
zeek.conn	583838
zeek.ssl	42389
zeek.files	21922
```

</details>

The above pipeline performs a full scan over the data at the node. Tenzir's
pipeline optimizer performs predicate push-down to avoid scans when possible.
Consider this pipeline:

```bash
tenzir '
  export
  | where *.id.orig_h in 10.0.0.0/8
  | write parquet to file local.parquet
  '
```

The optimizer coalesces the `export` and `where` operators such that
[expression](language/expressions.md) `*.id.orig_h in 10.0.0.0/8` gets pushed
down to the index and storage layer.

## Up Next

Now that you got a first impression of Tenzir pipelines, dive deeper by

- following the [user guides](user-guides.md) with step-by-step tutorials of
  common use cases
- learning more about the [language](language.md), [operators](operators.md),
  [connectors](connectors.md), [formats](formats.md), and the [data
  model](data-model.md)
- understanding [why](why-tenzir.md) we built Tenzir and how it compares to
  other systems

We're here to help! If you have any questions, swing by our friendly [community
Discord](/discord) or open a [GitHub
discussion](https://github.com/tenzir/tenzir/discussions).
