# fluent-bit

The `fluent-bit` source receives events from [Fluent
Bit](https://docs.fluentbit.io/).

## Synopsis

```
fluent-bit [-X|--set <key=value>,...] <plugin> [<key=value>...]
```

## Description

The `fluent-bit` source operator acts as a bridge into the Fluent Bit ecosystem,
making it possible to acquire events from a Fluent Bit [input plugin][inputs].

[inputs]: https://docs.fluentbit.io/manual/pipeline/inputs

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

The name of the Fluent Bit [input plugin][inputs].

Run `fluent-bit -h` and look under the **Inputs** section of the help text for
available plugin names. The web documentation often comes with an example
invocation near the bottom of the page, which also provides a good idea how you
could use the operator.

### `<key=value>`

Sets a plugin configuration property.

The positional arguments of the form `key=value` are equivalent to the
multi-option `-p key=value` of the `fluent-bit` executable.

## Examples

Ingest [OpenTelemetry](https://docs.fluentbit.io/manual/pipeline/inputs/slack)
logs, metrics, and traces:

```
fluent-bit opentelemetry
```

You can then send JSON-encoded log data to a freshly created API endpoint:

```bash
curl \
  --header "Content-Type: application/json" \
  --request POST \
  --data '{"resourceLogs":[{"resource":{},"scopeLogs":[{"scope":{},"logRecords":[{"timeUnixNano":"1660296023390371588","body":{"stringValue":"{\"message\":\"dummy\"}"},"traceId":"","spanId":""}]}]}]}' \
  http://0.0.0.0:4318/v1/logs
```

Handle [Splunk](https://docs.fluentbit.io/manual/pipeline/inputs/splunk) HTTP
HEC requests:

```
fluent-bit splunk port=8088
```

Handle [ElasticSearch &
OpenSearch](https://docs.fluentbit.io/manual/pipeline/outputs/elasticsearch)
Bulk API requests or ingest from beats (e.g., Filebeat, Metricbeat, Winlogbeat):

```
fluent-bit es port=9200
```
