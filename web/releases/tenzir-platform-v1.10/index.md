---
title: "Tenzir Platform v1.10: A New Explorer"
slug: tenzir-platform-v1.10
authors: [lava]
date: 2025-05-16
tags: [release, platform]
comments: true
---

Today, we're releasing [Tenzir Platform v1.10][github-release], which introduces
a restructed page layout and the ability to statically define workspaces
in on-prem environments.

![Tenzir Platform v1.10](tenzir-platform-v1.10.svg)

[github-release]: https://github.com/tenzir/platform/releases/tag/v1.10.0

<!-- truncate -->

## Restructured Nodes Page

This release reorganizes the page layout to improve usability
and provide a clearer overview of the current state of a Tenzir Node.

We combined the Explorer, Pipelines, and Contexts tabs into a single, top-level
Nodes tab. Additionally, we moved the Installed Packages tab from the library
into this new Nodes page.

![Nodes Page Layout](layout.png)

All node-related information is now consolidated on a single page, making it
easier to access at a glance.

We plan to add more utilites to the nodes page soon, providing deeper insights
into the state of your fleet.

We also redesigned the library, allowing you to filter by the subcategories
Sources, Destinations, Mappings and Contexts.

![Library Page Layout](library.png)

This makes it easier to find exactly the package you're looking for.

## Static Workspaces and Ephemeral Nodes

Sovereign Edition users can now define workspaces in a static configuration
provided to the Tenzir Platform instance.

```yaml
---
workspaces:
  static0:
    name: Tenzir
    category: Statically Configured Workspaces
    icon-url: https://storage.googleapis.com/tenzir-public-data/icons/tenzir-logo-square.svg
    auth-rules:
      - {"auth_fn": "auth_allow_all"}
```

:::tip Generating Auth Rules
Use the new `print-auth-rule` CLI command to easily generate auth rules in
the correct format. For example, to get the rule above you can
run `tenzir-platform tools print-auth-rule allow-all`.
:::

The `platform` service in a Tenzir Platform deployment uses the `WORKSPACE_CONFIG_FILE` environment variable to locate the static workspace configuration file.

```yaml
# docker-compose.yaml
services:
  platform:
    environment:
      # [...]
      - WORKSPACE_CONFIG_FILE=/etc/tenzir/workspaces.yaml
    volumes:
      - ./workspaces.yaml:/etc/tenzir/workspaces.yaml
```

### Ephemeral Nodes

You can define a workspace token for statically configured workspaces.
This shared secret allows any Tenzir Node with the token to connect to
the workspace.

```yaml
workspaces:
  static0:
    token: wsk_e9ee76d4faf4b213745dd5c99a9be11f501d7009ded63f2d5NmDS38vXR
```

Please note that a valid workspace token must follow a specific format.
To obtain a suitable token for your workspace, use
the `tenzir-platform tools generate-workspace-token` command.

Instead of writing out the workspace token in plain text, you can
specify a file that contains the token:

```yaml
workspaces:
  static0:
    token-file: /run/secrets/workspace_token
```

A Tenzir Node can register itself at the Tenzir Platform dynamically
if it knows the workspace token:

```bash
$ cat config.yaml
tenzir:
  token: wsk_e9ee76d4faf4b213745dd5c99a9be11f501d7009ded63f2d5NmDS38vXR
  platform-control-endpoint: http://tenzir-platform.example.org:3001

$ tenzir-node --config=config.yaml
```

Nodes connected this way are treated as *ephemeral*, meaning they're
not permanently added to the workspace but will disappear as soon
as the connection ends.

### Static Dashboards

You can define a static set of dashboards for a workspace.
To do so, use the `dashboards` YAML key:

```yaml
workspaces:
  static0:
    dashboards:
      dashboard1:
        name: Example Dashboard
        cells:
          - name: Dashboard 1
            definition: |
              partitions
              where not internal
              summarize events=sum(events), schema
              sort -events
            type: table
            x: 0
            y: 0
            w: 12
            h: 12
```

:::note Dashboard Coordinates
Dashboards are arranged in a virtual grid of width 24. Ensure that
`x + w <= 24` when setting dashboard coordinates.
:::note

While it is possible to update dashboards defined like this
at runtime, they reset to their original state every time the platform
restarts.


## Other Changes

 - We replaced the `--dry-run` option for the `tenzir-platform admin add-auth-rule`
   commands with the new `tenzir-platform tools print-auth-rule` commands.
 - [...]


## Join Us for Office Hours

Join us for our bi-weekly office hours every other Tuesday at 5 PM CET on our
[Discord server][discord]. It's a great opportunity to share your experiences,
ask questions, and help shape the future of Tenzir with your valuable feedback!

[discord]: /discord
