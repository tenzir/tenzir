# fluent-bit

The `fluent-bit` sink sends events to [Fluent Bit](https://docs.fluentbit.io/).

## Synopsis

```
fluent-bit [-X|--set <key=value>,...] <plugin> [<key=value>...]
```

## Description

The `fluent-bit` sink operator acts as a bridge into the Fluent Bit ecosystem,
making it possible to process events with a Fluent Bit [output plugin][outputs].

[outputs]: https://docs.fluentbit.io/manual/pipeline/output

Syntactically, the `fluent-bit` operator behaves similar to an invocation of the
`fluent-bit` command line utility. For example, the invocation

```bash
fluent-bit -o plugin -p key1=value1 -p key2=value2 -p ...
```

translates to our `fluent-bit` operator as follows:

```bash
fluent-bit plugin key1=value1 key2=value2 ...
```

### `-X|--set <key=value>`

A comma-separated list of key-value pairs that represent the global properties
of the Fluent Bit service., e.g., `-X flush=1,grace=3`.

Consult the list of available [key-value pairs][service-properties] to configure
Fluent Bit according to your needs.

[service-properties]: https://docs.fluentbit.io/manual/administration/configuring-fluent-bit/classic-mode/configuration-file#config_section

We recommend factoring these options into the plugin-specific `fluent-bit.yaml`
so that they are independent of the `fluent-bit` operator arguments.

### `<plugin>`

The name of the Fluent Bit [output plugin][outputs].

Run `fluent-bit -h` and look under the **Outputs** section of the help text for
available plugin names. The web documentation often comes with an example
invocation near the bottom of the page, which also provides a good idea how you
could use the operator.

### `<key=value>`

Sets a plugin configuration property.

The positional arguments of the form `key=value` are equivalent to the
multi-option `-p key=value` of the `fluent-bit` executable.

## Examples

Send events to [Slack](https://docs.fluentbit.io/manual/pipeline/outputs/slack):

```
fluent-bit slack webhook=https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX
```

Send events to
[Splunk](https://docs.fluentbit.io/manual/pipeline/outputs/splunk):

```
fluent-bit splunk host=127.0.0.1 port=8088 tls=on tls.verify=off splunk_token=11111111-2222-3333-4444-555555555555
```

Send events to
[ElasticSearch](https://docs.fluentbit.io/manual/pipeline/outputs/elasticsearch):

```
fluent-bit es host=192.168.2.3 port=9200 index=my_index type=my_type
```
