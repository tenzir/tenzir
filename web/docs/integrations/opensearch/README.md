# OpenSearch

[OpenSearch](https://opensearch.org) is a search and observability suite for
unstructured data. Tenzir can send events to OpenSearch.

![OpenSearch](opensearch.svg)

The [`to_opensearch`](../../tql2/operators/to_opensearch.md) output operator
makes it possible to send structured data to OpenSearch via the [Bulk
API](https://opensearch.org/docs/latest/api-reference/document-apis/bulk/).
Tenzir attempts to maximally batch events for throughput, accumulating multiple
events before shipping them within a single API call. You can control batching
behavior with the `max_content_length` and `send_timeout` options.

## Examples

### Send events to an OpenSearch index

```tql
from {event: "example"}
to_opensearch "localhost:9200", action="create", index="main"
```

Replace `localhost` with the IP address of your OpenSearch instance.

For more details, see the documentation for the
[`to_opensearch`](../../tql2/operators/to_opensearch.md) operator.

### Selectively specify metadata and document

Instead of treating the entire event as document to be indexed by OpenSearch,
you can designate a nested record as document:

```tql
from {category: "qux", doc_id: "XXX", event: {foo: "bar"}}
to_opensearch "localhost:9200", id=doc_id, doc=event, action="update", index=category
```

The above example updates the document with ID `XXX` with the contents from the
nested field `event`.
