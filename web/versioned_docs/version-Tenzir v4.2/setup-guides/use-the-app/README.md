---
sidebar_position: 0
---

# Use the app

The Tenzir "app" is the cloud-native management platform at
[app.tenzir.com](https://app.tenzir.com) to interact with your pipelines and
nodes.

:::note Editions
You can use the app with the free *Community Edition* and the paid *Enterprise
Edition*. Visit [our website](https://tenzir.com/pricing) for a detailed
comparison of available editions.
:::

## Create an account

Everyone can create a free account in a few steps:

1. Go to [app.tenzir.com](https://app.tenzir.com)
2. Sign in with your identity provider or create an account

## Get to know the app

The app consists of the following pages. We'll briefly explain what they are and
then work with them.

### Overview

The overview page allows you to manage your nodes and pipelines.

![Overview](app-overview.excalidraw.svg)

After you've created a node view the [configurator](#configurator), it will show
up in NODES. You can click on a node and will see all pipelines from it in
PIPELINES.

### Explorer

The explorer allows you to interact with your data by [running
pipelines](../../user-guides/run-a-pipeline/README.md).

![Explorer](app-explorer.excalidraw.svg)

#### Run a pipeline

Write your pipeline in EDITOR, hit the *Run* button, and analyze the result in
SCHEMAS and DATA.

#### Deploy a pipeline

Create a "managed" pipeline by hitting the *Deploy* button. You can only deploy
*closed* pipelines, i.e., it must have a
[source](../../operators/sources/README.md) and a
[sink](../../operators/sources/README.md) operator.

### Configurator

The configurator allows you to create a node-specific configuration file and
download it.

![Configurator](app-configurator.excalidraw.svg)

Follow INSTRUCTIONS for a concrete sequence of steps to deploy your node. Once
your node connects, you get a notification and see it in [overview](#overview).

## Delete your account

You can always delete your account:

1. Go to your [Account](https://app.tenzir.com/account)
2. Click *Delete Account*

:::warning
Deleting your account will remove all data about you from our cloud platform.
You will also lose the ability to manage pipelines on your node.
:::

If you decide to come back just re-create an account [as described
above](#create-an-account).
