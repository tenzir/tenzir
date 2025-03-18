# OpenSearch

[OpenSearch](https://opensearch.org) is a search and observability suite for
unstructured data. Tenzir can send events to OpenSearch.

![OpenSearch](opensearch.svg)

When sending data to Elasticsearch, Tenzir uses the [Bulk
API](https://opensearch.org/docs/latest/api-reference/document-apis/bulk/)
and attempts to maximally batch events for throughput, accumulating multiple
events before shipping them within a single API call. You can control batching
behavior with the `max_content_length` and `send_timeout` options.

For more details, see the documentation for the
[`to_opensearch`](../../tql2/operators/to_opensearch.mdx) operator.

## Examples

### Send events to an OpenSearch index

```tql
from {event: "example"}
to "opensearch://localhost:9200", action="create", index="main"
```

Replace `localhost` with the IP address of your OpenSearch instance.

### Selectively specify metadata and document

Instead of treating the entire event as document to be indexed by OpenSearch,
you can designate a nested record as document:

```tql
from {category: "qux", doc_id: "XXX", event: {foo: "bar"}}
to "opensearch://localhost:9200", id=doc_id, doc=event, action="update", index=category
```

The above example updates the document with ID `XXX` with the contents from the
nested field `event`.
