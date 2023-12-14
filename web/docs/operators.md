# Operators

An *operator* is the execution building block of a [pipeline](pipelines.md).

There exist three primary types:

1. **Source**: produces data
2. **Sink**: consumes data
3. **Transformation**: produces and consumes data

Every operator have an *input type* and *output type*:

![Operator Types](operator-types.excalidraw.svg)

Operators can be *polymorphic* in that they can have more than a single input
and output type. For example, `head` accepts both `bytes` and `events`,
filtering either the first N bytes or events.

Additionally, [user-defined operators](language/user-defined-operators.md) add
an alias for a pipeline to make it usable as an operator.

import DocCardList from '@theme/DocCardList';

<DocCardList />
