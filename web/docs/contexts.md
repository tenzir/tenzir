# Contexts

A context is a stateful object that can be used for enrichment in pipelines. You
can manage a context instance with the [`context`](operators/context.md)
operator and use it with the [`enrich`](operators/enrich.md) operator in one or
more other pipelines.

![Contextualization](contextualization.excalidraw.svg)

Each context has a specific *type* that needs to be specified upon construction.
Each context type defines how it processes updates and how it enriches. For
example, the [lookup table](contexts/lookup-table.md) context extends events
by performing a key-based lookup in a hash table.

The list below shows all available context types. For a more in-depth
introduction into the contextualization framework, please refer to our blog post
[Contextualization Made Simple](/blog/contextualization-made-simple).

To create a context, use the [`context create`](operators/context.md) operator
or configure the context as part of your configuration file:

```yaml {0} title="<prefix>/etc/tenzir/tenzir.yaml"
tenzir:
  contexts:
    # A unique name for the context that's used in the context, enrich, and
    # lookup operators to refer to the context.
    indicators:
      # The type of the context.
      type: bloom-filter
      # Arguments for creating the context, depending on the type. Refer to the
      # documentation of the individual context types to see the arguments they
      # require. Note that changes to these arguments do not apply to any
      # contexts that were previously created.
      arguments:
        capacity: 1B
        fp-probability: 0.001
      # Disables the context.
      disabled: false
```

import DocCardList from '@theme/DocCardList';

<DocCardList />
