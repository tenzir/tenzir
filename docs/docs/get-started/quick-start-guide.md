# Quick Start Guide
Want to get some hands-on experience with VAST quickly? Read on. This guide
takes you through a tour of the key use cases.

## Run VAST

Experimenting with a static binary or Docker image is often the easiest way to
get started. Read our [setup instructions](/vast/docs/setup-vast/) for a more
elaborate route.

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="static" label="Static Binary" default>
If you have a Linux at your fingertips, just download and extract our package
with a static binary:

```bash
mkdir /opt/vast
cd /opt/vast
wget https://github.com/tenzir/vast/releases/latest/download/vast-linux-static.tar.gz
mkdir -p /opt/vast
tar xzf vast-linux-static.tar.gz -C /opt/vast
export PATH="/opt/bin/vast:$PATH" # based on your shell, e.g., fish_add_path /opt/bin/vast
vast start
```
</TabItem>
<TabItem value="docker" label="Docker">
For a container deployment, use our official Docker image:

```bash
docker pull tenzir/vast
mkdir -p /tmp/db # persistent state
docker run -dt --name=vast --rm -p 42000:42000 -v /tmp/db:/var/lib/vast \
  tenzir/vast start
```
</TabItem>
</Tabs>

Now that you have running VAST node, you can start ingesting data.

## Ingest Data

TODO
