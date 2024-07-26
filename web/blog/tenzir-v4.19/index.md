---
title: Tenzir v4.19
authors: [lava]
date: 2024-07-25
tags: [release, packages, python]
comments: true
---

[Tenzir v4.19][github-release] now supports installing pipelines and contexts
together in packages, an all-new mechanism that makes installing integrations
easier than before.

![Tenzir v4.19](tenzir-v4.19.excalidraw.svg)

[github-release]: https://github.com/tenzir/tenzir/releases/tag/v4.19.0

<!-- truncate -->

## Packages

Packages are the evolution of [Pipelines as Code][pipelines-as-code]. The idea
is simple: Take a set of pipelines and contexts that thematically belong
together, and deploy them together in one unit.

Installing a package is as simple as running this pipeline:

```text {0} title="Install a package from a file"
from path/to/package.yaml
| package add
```

This leverages the [`package` operator](/next/operators/package), which has two
modes of operation: `package add` and `package remove`.

To list all installed packages, run `show packages`. Listing pipelines or
contexts with `show pipelines` and `show contexts` contains an additional
`package` field to identify pipelines and packages installed through a package.

If you prefer infrastructure as code for your deployments, you can install any
package into `<config-dir>/package/<package-name>/package.yaml`, which the node
reads when starting up.

Let's walk through this by writing a package that offers a neat integration with
the [Feodo Tracker Blocklist](https://feodotracker.abuse.ch/blocklist/) by
integrating the data into a context.

We start our package by assigning some metadata:

```yaml {0} title="feodo/package.yaml [1/5]"
id: feodo
name: Feodo Abuse Blocklist
author: Tenzir
author_icon: https://github.com/tenzir.png
package_icon: null
description: |
  Feodo Tracker is a project of abuse.ch with the goal of sharing botnet C&C
  servers associated with Dridex, Emotet (aka Heodo), TrickBot, QakBot (aka
  QuakBot / Qbot) and BazarLoader (aka BazarBackdoor). It offers various
  blocklists, helping network owners to protect their users from Dridex and
  Emotet/Heodo.
```

Every package must have a unique identifier. We recommend setting the package
name, description and author, and we also recommend setting an author and a
package icon where possible.

Packages may define inputs, which are user-defined variables that can
be referenced in pipeline and context definitions. For this package,
we don't define any inputs:

```yaml {0} title="feodo/package.yaml [2/5]"
inputs: {}
```

Packages may define any number of contexts. For our Feodo Abuse Blocklist
package we'll define a context named `feodo` as a [Lookup
Table](/contexts/lookup-table). We recommend writing a description for every
context.

```yaml {0} title="feodo/package.yaml [3/5]"
contexts:
  feodo:
    type: lookup-table
    description: |
      A lookup table that contains the elements of the feodo IP blocklist.
```

Packages may define any number of pipelines. These pipelines get automatically
started when the package is installed. For our example, let's add a pipeline
that ensures that our `feodo` context is continuously updated:

```yaml {0} title="feodo/package.yaml [4/5]"
pipelines:
  update-context:
    name: Update Feodo Context
    description: |
      Periodically refresh the Feodo lookup-table context.
    definition: |
      every 1 hour from https://feodotracker.abuse.ch/downloads/ipblocklist_aggressive.csv read csv --allow-comments
      | context update feodo --key dst_ip
```

The format for pipelines matches the format for [Pipelines as
Code][pipelines-as-code].

Lastly, we recommend adding snippets to your package that show how to use
it:

```yaml {0} title="feodo/package.yaml [5/5]"
snippets:
  - name: Match historical and live data against the `feodo` context
    description: |
      Find persisted events that have an IP address matching the `feodo`
      context.
    definition: |
      lookup feodo --field :ip
  - name: Visualize successful lookups with the `feodo` context in the last week
    description: |
      Creates a stacked area chart that shows the number of hourly hits of
      pipelines using the `lookup` operator with the `feodo` context.
    definition: |
      metrics lookup
      | where context == "feodo"
      | where timestamp > 7d ago
      | summarize retro_hits=sum(retro.hits), live_hits=sum(live.hits) by timestamp resolution 1h
      | sort timestamp
      | chart area --position stacked
```

That's it! Our own package, all done and wrapped up.

:::tip Want to dive deeper?
We've prepared some reading material:
- Take a look at the [`package` operator's
  documentation](/next/operators/package).
- Read the user guide on [installing a
  package](/next/user-guides/install-a-package).
:::

[pipelines-as-code]: /user-guides/run-pipelines#as-code
[feodotracker-blocklist]: https://feodotracker.abuse.ch/blocklist

## Improving the Python Operator

The `python` operator got a revamp. It now relies on the excellent
[uv](https://github.com/astral-sh/uv) package installer, rather than
`python-pip`, reducing startup time significantly.

If your installation relies on a custom `pip.conf` file, we recommend [migrating
to a `uv.toml` configuration file](https://github.com/astral-sh/uv/issues/1404).

Because of the reduced startup time, the operator no longer shares virtual
environments between pipelines. This means that on every start of your pipeline,
we will ensure that the most recent versions of all packages are installed.

:::tip Clean up old virtual environments
Previous versions of the operator did not clean up virtual environments on their
own. If you installed Tenzir on bare metal, we recommend removing old virtual
environments manually. They are located at
`<cache-directory>/tenzir/python/venvs`, which will be
`/var/cache/tenzir/python/venvs` for most deployments.
:::

## Surviving Spikes with the Buffer Operator

The `buffer` operator is a new addition that makes it possible to break back
pressure in pipelines.

:::info What is back pressure?
Operators in a pipeline communicate in both directions: The operator's output is
sent downstream to the next operator, and an operator can emit demand to the
upstream operator. Demand controls whether an operator gets scheduled—that is,
an operator that has no demand to produce any output just doesn't run at all
anymore. This mechanism is called back pressure.
:::

Most of the time, back pressure is very useful: It makes it so that your
pipeline does no unnecessary work, and so that events do not pile up in memory
when an operator is slow.

However, some data sources really do not like to be throttled. For example, when
reading from a UDP connection, throttling the source effectively means losing
events.

The `buffer` operator is a special operator that doesn't quite follow the rules
other operators need to abide by. The operator has two policies: `block` and
`drop`. With the `block` policy, the operator stops emitting demand upstream
only when the buffer is full. With the `drop` policy, the operator never stops
emitting demand upstream, but then drops events if the buffer is full.

For example, let's say we acquire syslog messages with a very high speed over UDP:

```text {0} title="Acquire data from syslog, buffering up to 1M events"
from udp://localhost:514 read syslog
| buffer 1M --policy drop
| …
```

The `buffer` operator emits metrics, so now we can also set up a chart that
monitors our buffer utilization:

```text {0}
metrics buffer
| where timestamp > 1 day ago
// substitute the id of the syslog pipeline here
| where pipeline_id == "<pipeline-id>"
| summarize used=max(used), free=min(free) by timestamp resolution 15min
| sort timestamp
| chart area --position stacked
```

## Other Changes

As usual, the [changelog][changelog] contains a full list of features, changes,
and bug fixes in this release.

Every second Tuesday at 8 AM EST / 11 AM EST / 5 PM CET / 9.30 PM IST, we hold
office hours in [our Discord server][discord]. Whether you have ideas for
packages, want to see a preview of what we plan to do with them in the app, an
idea that you'd like to discuss with us—come join and have a chat with us!

[discord]: /discord
[changelog]: /changelog#v4190
