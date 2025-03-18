# Elasticsearch

[ElasticSearch](https://elastic.co/elasticsearch) is a search and observability
suite for unstructured data. Tenzir can send events to Elasticsearch.

![Elasticsearch](elasticsearch.svg)

When sending data to Elasticsearch, Tenzir uses the [Bulk
API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html)
and attempts to maximally batch events for throughput, accumulating multiple
events before shipping them within a single API call. You can control batching
behavior with the `max_content_length` and `send_timeout` options.

:::info Advanced Details
For more details and possible configuration, see the documentation for the
[`to_opensearch`](../../tql2/operators/to_opensearch.mdx) operator. When you use
[`to`](../../tql2/operators/to.md) with the `elasticsearch://` URL scheme, the
pipeline uses the `to_opensearch` operator under the hood.
:::

## Examples

### Send events to an Elasticsearch index

```tql
from {event: "example"}
to "elasticsearch://localhost:9200", action="create", index="main"
```

Replace `localhost` with the IP address of your Elasticsearch instance.

### Selectively specify metadata and document

Instead of treating the entire event as document to be indexed by Elasticsearch,
you can designate a nested record as document:

```tql
from {category: "qux", doc_id: "XXX", event: {foo: "bar"}}
to "elasticsearch://localhost:9200", id=doc_id, doc=event, action="update", index=category
```

The above example updates the document with ID `XXX` with the contents from the
nested field `event`.
