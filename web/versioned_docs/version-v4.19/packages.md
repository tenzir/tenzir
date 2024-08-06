# Packages

A **package** is a set of pipelines and contexts that are thematically related
and deployed together as a single unit.

## Anatomy of a Package

A package definition consists of three major parts.

1. Package metadata, for example the name, author and description of the
   package.
2. Snippets, which are example pipelines for use with the Explorer on
   [app.tenzir.com](https://app.tenzir.com/explorer) that demonstrate how to use
   the package.
3. A set of inputs provided by the user to customize the package installation
   to their environment.
4. A set of pipelines and contexts that make up the contents of the package.

### Package Metadata

Packages start with a set of metadata describing the package.

```yaml
# The unique id of the package (required).
id: feodo

# The display name of the package and a path to an icon for the package.
name: Feodo Abuse Blocklist
package_icon: null

# The display name of the package author and a path to a profile picture.
author: Tenzir
author_icon: https://github.com/tenzir.png

# A user-facing description of the package.
description: |
  Feodo Tracker is a project of abuse.ch with the goal of sharing botnet C&C
  servers associated with Dridex, Emotet (aka Heodo), TrickBot, QakBot (aka
  QuakBot / Qbot) and BazarLoader (aka BazarBackdoor). It offers various
  blocklists, helping network owners to protect their users from Dridex and
  Emotet/Heodo.
```

### Snippets

Packages may include snippets that showcase how to use the package, e.g., by
showing how to display a chart that visualizes data imported by a package, or by
providing a set of usage examples for data provided by a package.

```yaml
# Snippets contain a name, a description, and a pipeline definition for use with
# the explorer on app.tenzir.com.
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

### Inputs

Packages may define inputs to allow customizing a package installation. This
allows the package definition to be independent of the user's local environment.

```yaml
# Define user inputs to customize the package installation.
inputs:
  # Every input must have a unique id.
  refresh-rate:
    # A user-facing name for the input (required).
    name: Refresh Rate
    # The type of the input (required). Currently, 'string' is the only
    # supported option.
    type: string
    # A user-facing description of the input.
    description: |
      The interval at which the Feodo Tracker context is refreshed. Defaults to
      refreshing every second.
    # An (optional) default value for the input. The input is required if there
    # is no input value.
    default: 1s
```

### Pipelines and Contexts

A package is a unit of pipelines and contexts that is deployed together. This is
where you define them:

```yaml
# Define any number of pipelines.
pipelines:
  update-context:
    # An optional user-facing name for the pipeline. Defaults to the id.
    name: Update Feodo Context
    # An optional user-facing description of the pipeline.
    description: |
      Periodically refresh the Feodo lookup-table context.
    # The definition of the pipeline. Configured pipelines that fail to start
    # cause the node to fail to start.
    definition: |
      every ${{ inputs.refresh-rate }} from https://feodotracker.abuse.ch/downloads/ipblocklist_aggressive.csv read csv --allow-comments
      | context update feodo --key dst_ip
    # Pipelines that encounter an error stop running and show an error state.
    # This option causes pipelines to automatically restart when they
    # encounter an error instead. The first restart happens immediately, and
    # subsequent restarts after the configured delay, defaulting to 1 minute.
    # The following values are valid for this option:
    # - Omit the option, or set it to null or false to disable.
    # - Set the option to true to enable with the default delay of 1 minute.
    # - Set the option to a valid duration to enable with a custom delay.
    restart-on-error: 1 minute
    # Disables the pipeline.
    disabled: false

# Define any number of contexts.
contexts:
  # A unique name for the context that's used in the context, enrich, and
  # lookup operators to refer to the context.
  feodo:
    # The type of the context (required).
    type: lookup-table
    # An optional user-facing description of the context.
    description: |
      A lookup table that contains the elements of the feodo IP blocklist.
    # Arguments for creating the context, depending on the type. Refer to the
    # documentation of the individual context types to see the arguments they
    # require. Note that changes to these arguments do not apply to any
    # contexts that were previously created.
    args: {}
    # Disables the context.
    disabled: false
```

:::tip Use packages
Start using packages by [installing one](installation/install-a-package.md).
:::
