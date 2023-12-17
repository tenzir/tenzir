# Splunk

The [Splunk](https://splunk.com) integration allows you to place Tenzir between
your data sources and existing Splunk deployment.

## Send data to an existing HEC endpoint

To send data from a pipeline to a Splunk [HTTP Event Collector (HEC)][hec]
endpoint, use the [`fluent-bit`](../operators/fluent-bit.md) sink operator.

For example, deploy the following pipeline to forward all
[Suricata](suricata.md) alerts arriving at a node to Splunk:

```
export --live
| where #schema == "suricata.alert"
| fluent-bit
    splunk
    host=1.2.3.4
    port=8088
    tls=on
    tls.verify=off
    splunk_token=TOKEN
```

Replace `1.2.3.4` with the IP address of your splunk host and `TOKEN` with your
HEC token.

For more details, read the official [Fluent Bit documentation of the Splunk
output][fluentbit-splunk-output].

## Spawn a HEC endpoint as pipeline source

To send data to a Tenzir pipeline instead of Splunk, you can open a Splunk [HTTP
Event Collector (HEC)][hec] endpoint using the
[`fluent-bit`](../operators/fluent-bit.md) source operator.

For example, to ingest all data into a Tenzir node instead of Splunk, point your
data source to the IP address of the Tenzir node at port 9880 by deploying this
pipeline:

```
fluent-bit splunk splunk_token=TOKEN
| import
```

Replace `TOKEN` with the Splunk token configured at your data source.

To listen on a different IP address, e.g., 1.2.3.4 add `listen=1.2.3.4` to the
`fluent-bit` operator.

For more details, read the official [Fluent Bit documentation of the Splunk
input][fluentbit-splunk-input].

[fluentbit-splunk-input]: https://docs.fluentbit.io/manual/pipeline/inputs/splunk
[fluentbit-splunk-output]: https://docs.fluentbit.io/manual/pipeline/outputs/splunk
[hec]: https://docs.splunk.com/Documentation/Splunk/latest/Data/UsetheHTTPEventCollector
