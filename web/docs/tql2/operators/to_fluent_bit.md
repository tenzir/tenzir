# to_fluent_bit

Sends events via [Fluent Bit](https://docs.fluentbit.io/).

## Synopsis

```tql
to_fluent_bit plugin:str, [options=record, fluent_bit_options=record]
```

## Description

The `to_fluent_bit` operator acts as a bridge into the Fluent Bit ecosystem,
making it possible to send events to Fluent Bit [output plugin][outputs].

[outputs]: https://docs.fluentbit.io/manual/pipeline/outputs

Syntactically, the `from_fluent_bit` operator behaves similar to an invocation of the
`fluent-bit` command line utility. For example, the invocation

```bash
fluent-bit -o plugin -p key1=value1 -p key2=value2 -p ...
```

translates to our `from_fluent_bit` operator as follows:

```tql
from_fluent_bit "plugin" options{ key1: value1, key2:value2 ... }
```

:::tip Read from Fluent Bit
You can acquire events from Fluent Bit using the [`from_fluent_bit` operator](from_fluent_bit.md).
:::

### `plugin:str`

The name of the Fluent Bit plugin.

Run `fluent-bit -h` and look under the **Outputs** section of the
help text for available plugin names. The web documentation often comes with an
example invocation near the bottom of the page, which also provides a good idea
how you could use the operator.

### `options=record (optional)`

Sets plugin configuration properties.

The key-value pairs in this record are equivalent to `-p key=value` for the
`fluent-bit` executable.

### `fluent_bit_options=record (optional)`

Sets global properties of the Fluent Bit service., e.g., `fluent_bit_options= {flush:1,grace=3}`.

Consult the list of available [key-value pairs][service-properties] to configure
Fluent Bit according to your needs.

[service-properties]: https://docs.fluentbit.io/manual/administration/configuring-fluent-bit/classic-mode/configuration-file#config_section

We recommend factoring these options into the plugin-specific `fluent-bit.yaml`
so that they are independent of the `fluent-bit` operator arguments.

## Examples

### ElasticSearch
Send events to
[ElasticSearch](https://docs.fluentbit.io/manual/pipeline/outputs/elasticsearch):

```tql
to_fluent_bit "es",
  options={
    host: 192.168.2.3,
    port: 9200,
    index: "my_index",
    type: "my_type"
  }
```

### Slack

Send events to [Slack](https://docs.fluentbit.io/manual/pipeline/outputs/slack):

```tql
let $slack_hook = "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"
to_fluent_bit "slack",
  options={
    webhook: $slack_hook
  }
```

### Splunk

:::tip Use the dedicated [`to_splunk` operator](to_splunk.md) instead
:::
