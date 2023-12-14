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

import DocCardList from '@theme/DocCardList';

<DocCardList />
