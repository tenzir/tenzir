# Packages

A **package** is a set of pipelines and contexts that are thematically related
and deployed together as a single unit.

:::tip Use packages
Start using packages by [installing one](installation/install-a-package.md).
:::

## Anatomy of a Package

A package definition consists of four major parts:

1. **Metadata**, for example the name, author and description of the
   package.
2. **Inputs** that parameterize the package installation to their environment.
3. **Pipelines and contexts** that make up the contents of the package.
4. **Examples** that demonstrate how to use the package.

### Metadata

Packages start with a set of metadata describing the package.

```yaml
# The unique ID of the package. (required)
id: example

# The display name of the package and a path to an icon for the package.
name: Example
package_icon: https://github.com/tenzir.png

# The display name of the package author and a path to a profile picture.
author: Tenzir
author_icon: https://github.com/tenzir.png

# A user-facing description of the package.
description: |
  **Lorem ipsum** dolor sit amet, consectetur adipiscing elit. Nullam suscipit
  lacus felis, ac lacinia nibh pretium ut. Curabitur congue aliquam neque.
  Vivamus in magna non turpis malesuada volutpat ut a felis. Ut lorem eros,
  vulputate eget finibus ut, posuere sed leo. Vestibulum porta laoreet
  venenatis. Curabitur aliquet semper sem, et tincidunt metus cursus at. Nulla
  dapibus nibh vel faucibus commodo. Sed euismod eu sapien ut dictum. Phasellus
  tincidunt venenatis semper.
```

### Inputs

Inputs are template variables that get replaced upon package installation. This
allows the package definition to be independent of the deployed environment.

```yaml
# Define user inputs to customize the package installation.
inputs:
  # Every input must have a unique id.
  refresh-rate:
    # A user-facing name for the input (required).
    name: Refresh Rate
    # A user-facing description of the input.
    description: |
      The interval at which we refresh our example context.
      Defaults to refreshing every second.
    # An (optional) default value for the input. The input is required if there
    # is no input value.
    default: 1s
```

Inputs can be referenced in pipeline and example definitions and in context
arguments with the syntax `{{ inputs.input-name }}`. They are replaced by their
configured values when installing a package. For example, with the input
configured as above the pipeline `every {{ inputs.refresh-rate }} { version }`
would print the version once per second by default.

To write double curly braces, use the syntax `{{ '{{' }}` to produce the
literal string enclosed inside the single quotes.

### Pipelines and Contexts

A package is a unit of pipelines and contexts that is deployed together. The
`pipelines` and `contexts` fields define them:

```yaml
# Define any number of pipelines.
pipelines:
  update-context:
    # An optional user-facing name for the pipeline. Defaults to the id.
    name: Update Example Context
    # An optional user-facing description of the pipeline.
    description: |
      Periodically refresh the Example lookup table.
    # The definition of the pipeline. Configured pipelines that fail to start
    # cause the node to fail to start.
    definition: |
      every ${{ inputs.refresh-rate }}
        from https://example.org/
          read csv --allow-comments
      | context update example --key foo
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
    # Unstoppable pipelines will run automatically and indefinitely.
    # They cannot be paused or stopped manually.
    # If they do complete, they will end up in a failed state.
    # If `restart-on-error` is enabled, they will restart after the specified
    # duration.
    unstoppable: true

# Define any number of contexts.
contexts:
  # A unique name for the context that's used in the context, enrich, and
  # lookup operators to refer to the context.
  example:
    # The type of the context (required).
    type: lookup-table
    # An optional user-facing description of the context.
    description: |
      **Lorem ipsum** dolor sit amet, consectetur adipiscing elit. Nullam
      suscipit lacus felis, ac lacinia nibh pretium ut. Curabitur congue aliquam
      neque. Vivamus in magna non turpis malesuada volutpat ut a felis. Ut lorem
      eros, vulputate eget finibus ut, posuere sed leo. Vestibulum porta laoreet
      venenatis. Curabitur aliquet semper sem, et tincidunt metus cursus at.
      Nulla dapibus nibh vel faucibus commodo. Sed euismod eu sapien ut dictum.
      Phasellus tincidunt venenatis semper.
      
    # Arguments for creating the context, depending on the type. Refer to the
    # documentation of the individual context types to see the arguments they
    # require. Note that changes to these arguments do not apply to any
    # contexts that were previously created.
    args: {}
    # Disables the context.
    disabled: false
```

### Examples

Packages may include examples that showcase how to use the package, e.g., by
showing how to display a chart that visualizes data imported by a package, or by
providing a set of usage examples for data provided by a package.

```yaml
# Examples contain a name, a description, and a pipeline definition for use with
# the explorer on app.tenzir.com.
examples:
  - name: Show example context details
    description: |
      This pipelines shows the contents of the example lookup table.
    definition: |
      context inspect example
  - name: |
      Visualize successful lookups with the `example` context in the last week
    description: |
      Creates a stacked area chart that shows the number of hourly hits of
      pipelines using the `lookup` operator with the `example` context.
    definition: |
      metrics lookup
      | where context == "example"
      | where timestamp > 7d ago
      | summarize retro_hits=sum(retro.hits), live_hits=sum(live.hits)
          by timestamp
          resolution 1h
      | sort timestamp
      | chart area --position stacked
```

## Configuration During Installation

During installation, the package definition gets merged with a
configuration object. This can happen in three ways:

1. In the [Tenzir Library](https://app.tenzir.com/library), you provide inputs
   that get converted into a `config` object.
2. Using the [`package_add`](../operators/package.md) operator, you construct a
   `config` record explicitly.
3. Using IaC-style installation, you provide a `config.yaml` with a `config`
   key.

Refer to the [package installation guide](installation/install-a-package.md) for
details on how each method works.

The `config` record has the following structure:

```yaml
config:
  # An optional string to identify the package version being installed.
  version: "4.0"
  # An unambigous description of the upstream source of this package.
  source:
    repository: https://github.com/tenzir/library
    directory: example/
    revision: 1274bd6042e4b74d07363643b5b01811e191b74c
  # A dictionary mapping input fields to their desired values.
  inputs:
    filename: /opt/example/data.tsv
  # A set of keys that override the corresponding fields in the package
  # definition.
  overrides:
    pipelines:
      example-pipeline:
        disabled: true
  # Opaque extra data that is ignored by the node.
  metadata:
    ansible:
      deployment_id: 483dej2
```

### Inputs

Pipeline and context definitions may contain references to template variables,
as in `from {{ inputs.filename }} version`, that are replaced by their
configured value when installing the package.

In order to provide non-default values for the defined inputs, use the
`config.inputs` key:

```yaml
config:
  inputs:
    filename: /opt/example/data.tsv
```

### Overrides

The `config.overrides` object is used to change the value of any field in the
package definition. This is intended to customize fields like `disabled` or
`restart-on-error` in pipeline definitions:

```yaml
config:
  overrides:
    pipelines:
      example-pipeline:
        disabled: true
```
