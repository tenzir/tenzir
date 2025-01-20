---
sidebar_position: 2
---

# Deploy a node

Deploying a node entails provisioning a node via the platform and then
installing it in your environment of choice.

## Provision a node

To deploy a self-hosted node, begin with provisioning one in the platform:

1. Visit https://app.tenzir.com/pipelines.
1. Click the *Add node* button in the left pane and select *self-hosted node*.
1. Enter a name for your node and click *Add node*.

## Install a node

Next, choose how you would like to deploy your node from the available options
below.

import DocCardList from '@theme/DocCardList';

<DocCardList />

## Configure a node

See the documentation on [configuration
files](../../configuration.md#configuration-files) along with the example
configuration to understand how you can configure yoru node.

:::note Accepting incoming connections
When your node starts it will listen for node-to-node connections on the TCP
endpoint `127.0.0.1:5158`. Select a different endpoint via the `tenzir.endpoint`
option. For example, to bind to an IPv6 address use `[::1]:42000`.

Set `tenzir.endpoint` to `false` to disable the endpoint, making the node
exclusively accessible through the Tenzir Platform. This effectively prevents
connections from other `tenzir` or `tenzir-node` processes.
:::
