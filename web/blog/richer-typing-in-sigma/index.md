---
title: Richer Typing in Sigma
description: Towards Native Sigma Rule Execution
authors: mavam
date: 2022-08-12
last_updated: 2023-02-12
tags: [sigma, regex, query-frontend]
---

VAST's [Sigma frontend](/docs/VAST%20v3.0/understand/language/frontends/sigma)
now supports more modifiers. In the Sigma language, modifiers transform
predicates in various ways, e.g., to apply a function over a value or to change
the operator of a predicate. Modifiers are the customization point to enhance
expressiveness of query operations.

The new [pySigma][pysigma] effort, which will eventually replace the
now-considered-legacy [sigma][sigma] project, comes with new modifiers as well.
Most notably, `lt`, `lte`, `gt`, `gte` provide comparisons over value domains
with a total ordering, e.g., numbers: `x >= 42`. In addition, the `cidr`
modifier interprets a value as subnet, e.g., `10.0.0.0/8`. Richer typing!

[sigma]: https://github.com/SigmaHQ/sigma
[pysigma]: https://github.com/SigmaHQ/pySigma

<!--truncate-->

How does the frontend work? Think of it as a parser that processes the YAML and
translates it into an expression tree, where the leaves are predicates with
typed operands according to VAST's data model. Here's how it works:

![Sigma Query Frontend](sigma-query-frontend.excalidraw.svg)

Let's take a closer look at some Sigma rule modifiers:

```yaml
selection:
  x|re: 'f(o+|u)'
  x|lt: 42
  x|cidr: 192.168.0.0/23
  x|base64offset|contains: 'http://'
```

The `|` symbol applies a modifier to a field. Let's walk through the above
example:

1. The `re` modifier changes the predicate operand from `x == "f(o+|u)"` to
   `x == /f(o+|u)/`, i.e., the type of the right-hand side changes from `string`
   to `pattern`.

2. The `lt` modifier changes the predicate operator from `==` to `<`, i.e.,
   `x == 42` becomes `x < 42`.

3. The `cidr` modifier changes the predicate operand to type subnet. In VAST,
   parsing the operand type into a subnet happens automatically, so the Sigma
   frontend only changes the operator to `in`. That is, `x == "192.168.0.0/23"`
   becomes `x in 192.168.0.0/23`. Since VAST supports top-k prefix search on
   subnets natively, nothing else needs to be changed.

   Other backends expand this to:

   ```c
   x == "192.168.0.*" || x == "192.168.1.*"
   ```

   This expansion logic on strings doesn't scale very well: for a `/22`, you
   would have to double the number of predicates, and for a `/21` quadruple
   them. This is where rich and deep typing in the language pays off.

4. `x`: there are two modifiers that operate in a chained fashion,
   transforming the predicate in two steps:

   1. Initial: `x == "http://"`
   2. `base64offset`: `x == "aHR0cDovL" || x == "h0dHA6Ly" || x == "odHRwOi8v"`
   3. `contains`: `x in "aHR0cDovL" || x in "h0dHA6Ly" || x in "odHRwOi8v"`

   First, `base64offset` always expands a value into a disjunction of 3
   predicates, each of which performs an equality comparison to a
   Base64-transformed value.[^1]

   Thereafter, the `contains` modifier translates the respective predicate
   operator from `==` to `in`. Other Sigma backends that don't support substring
   search natively transform the value instead by wrapping it into `*`
   wildcards, e.g., translate `"foo"` into `"*foo*"`.

[^1]: What happens under the hood is a padding a string with spaces. [Anton
Kutepov's article][sigma-article] illustrates how this works.

[sigma-article]: https://tech-en.netlify.app/articles/en513032/index.html

Our ultimate goal is to support a fully function executional platform for Sigma
rules. The table below shows the current implementation status of modifiers,
where ✅ means implemented, 🚧 not yet implemented but possible, and ❌ not yet
supported by VAST's execution engine:

|Modifier|Use|sigmac|VAST|
|--------|---|:----:|:--:|
|`contains`|perform a substring search with the value|✅|✅|
|`startswith`|match the value as a prefix|✅|✅|
|`endswith`|match the value as a suffix|✅|✅|
|`base64`|encode the value with Base64|✅|✅
|`base64offset`|encode value as all three possible Base64 variants|✅|✅
|`utf16le`/`wide`|transform the value to UTF16 little endian|✅|🚧
|`utf16be`|transform the value to UTF16 big endian|✅|🚧
|`utf16`|transform the value to UTF16|✅|🚧
|`re`|interpret the value as regular expression|✅|🚧
|`cidr`|interpret the value as a IP CIDR|❌|✅
|`all`|changes the expression logic from OR to AND|✅|✅
|`lt`|compare less than (`<`) the value|❌|✅
|`lte`|compare less than or equal to (`<=`) the value|❌|✅
|`gt`|compare greater than (`>`) the value|❌|✅
|`gte`|compare greater than or equal to (`>=`) the value|❌|✅
|`expand`|expand value to placeholder strings, e.g., `%something%`|❌|❌

Aside from completing the implementation of the missing modifiers, there are
three missing pieces for Sigma rule execution to become viable in VAST:

1. **Regular expressions**: VAST currently has no efficient mechanism to execute
   regular expressions. A regex lookup requires a full scan of the data.
   Moreover, the regular expression execution speed is abysimal. But we are
   aware of it and are working on this soon. The good thing is that the
   complexity of regular expression execution over batches of data is
   manageable, given that we would call into the corresponding [Arrow Compute
   function][arrow-containment-tests] for the heavy lifting. The number one
   challenge will be reduing the data to scan, because the Bloom-filter-like
   sketch data structures in the catalog cannot handle pattern types. If the
   sketches cannot identify a candidate set, all data needs to be scanned,

   To alleviate the effects of full scans, it's possible to winnow down the
   candidate set of partitions by executing rules periodically. When making the
   windows asymptotically small, this yields effectively streaming execution,
   which VAST already supports in the form of "live queries".

2. **Case-insensitive strings**: All strings in Sigma rules are case-insensitive
   by default, but VAST's string search is case-sensitive. As a workaround, we
   could translate Sigma strings into regular expressions, e.g., `"Foo"` into
   `/Foo/i`. Unfortunately there is a big performance gap between string
   equality search and regular expression search. We will need to find a better
   solution for production-grade rule execution.

3. **Field mappings**: while Sigma rules execute already syntactically, VAST
   currently doesn't touch the field names in the rules and interprets them as
   [field extractors][field-extractors]. In other words, VAST doesn't support
   the Sigma taxonomy yet. Until we provide the mappings, you can already write
   generic Sigma rules using [concepts][concepts].

[arrow-containment-tests]: https://arrow.apache.org/docs/VAST%20v3.0/cpp/compute.html#containment-tests
[field-extractors]: https://vast.io/docs/VAST%20v3.0/understand/language/expressions#field-extractor
[concepts]: https://vast.io/docs/VAST%20v3.0/understand/data-model/taxonomies#concepts

Please don't hesitate to swing by our [community chat](/discord)
and talk with us if you are passionate about Sigma and other topics around open
detection and response.
