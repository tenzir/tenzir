# fluentbit

The `fluentbit` sink sends events to [Fluent Bit](https://docs.fluentbit.io/).

## Synopsis

```
fluentbit <plugin> [<key=value>...]
```

## Description

The `fluentbit` sink operator acts as a bridge into the Fluent Bit ecosystem,
making it possible to process events with a Fluent Bit [output plugin][outputs]

[outputs]: https://docs.fluentbit.io/manual/pipeline/output

Syntactically, the `fluentbit` operator behaves similar to an invocation of the
`fluent-bit` command line utility. For example, the invocation

```bash
fluent-bit -o plugin -p key1=value1 -p key2=value2 -p ...
```

translates to our `fluentbit` operator as follows:

```bash
fluentbit plugin key1=value1 key2=value2 ...
```

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
fluentbit slack webhook=https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX
```

Send events to [Splunk](https://docs.fluentbit.io/manual/pipeline/outputs/splunk):

```
fluentbit splunk host=127.0.0.1 port=8088 tls=on tls.verify=off
```

Send events to [ElasticSearch](https://docs.fluentbit.io/manual/pipeline/outputs/elasticsearch):

```
fluentbit es host=192.168.2.3 port=9200 index=my_index type=my_type
```
