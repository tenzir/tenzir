# Setup

This section describes Tenzir from an **operator perspective**. We cover the
different stages of the setup process that ultimately yield a running Tenzir
instance. You have several options to enter the setup pipeline, based on what
intermediate artifact you would like to begin with.

```mermaid
flowchart LR
  classDef action fill:#00a4f1,stroke:none,color:#eee
  classDef artifact fill:#bdcfdb,stroke:none,color:#222
  %% Actions
  download(Download):::action
  build(Build):::action
  install(Install):::action
  deploy(Deploy):::action
  configure(Configure):::action
  tune(Tune):::action
  monitor(Monitor):::action
  %% Artifacts
  source([Source Code]):::artifact
  binary([Binary]):::artifact
  deployable([Package/Image]):::artifact
  instance([Instance]):::artifact
  %% Edges
  download --> source
  download --> binary
  download --> deployable
  source --> build
  build --> binary
  binary --> install
  install --> deployable
  deployable --> deploy
  deploy --> instance
  instance <--> configure
  instance <--> tune
  instance <--> monitor
  %% Links
  click download "setup/download" "Download Tenzir"
  click build "setup/build" "Build Tenzir"
  click install "setup/install" "Install Tenzir"
  click deploy "setup/deploy" "Deploy Tenzir"
  click configure "setup/configure" "Configure Tenzir"
  click tune "setup/tune" "Tune Tenzir"
  click monitor "setup/monitor" "Monitor Tenzir"
```

:::tip Quick Start
Want hands-on experience with Tenzir? Then continue with a quick tour below. 👇
:::

## Run Tenzir

To get up and running quickly, we recommend using the static binary or Docker
image:

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="static" label="Static Binary" default>
If you have a Linux at your fingertips, just download and extract our package
with a static binary:

```bash
mkdir /opt/tenzir
cd /opt/tenzir
wget https://github.com/tenzir/tenzir/releases/latest/download/tenzir-linux-static.tar.gz
mkdir -p /opt/tenzir
tar xzf tenzir-linux-static.tar.gz -C /opt/tenzir
export PATH="/opt/tenzir/bin:$PATH" # based on your shell, e.g., fish_add_path /opt/tenzir/bin
tenzir start
```
</TabItem>
<TabItem value="docker" label="Docker">
For a container deployment, use our official Docker image:

```bash
docker pull tenzir/tenzir
mkdir -p /tmp/db # persistent state
docker run -dt --name=tenzir --rm -p 5158:5158 -v /tmp/db:/var/lib/tenzir \
  tenzir/tenzir start
```
</TabItem>
</Tabs>

Now that you have running Tenzir node, you can start ingesting data.
