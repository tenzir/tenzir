---
draft: true
title: VAST v2.3
description: VAST v2.3 - Sigma modifiers
authors: mavam
tags: [release, sigma]
---

[VAST v2.3][github-vast-release] is out!

[github-vast-release]: https://github.com/tenzir/vast/releases/tag/v2.3.0

<!--truncate-->

## More Sigma rule modifiers

VAST's [Sigma frontend](/docs/understand-vast/query-language/frontends/sigma)
now supports more modifiers. In the Sigma language, modifiers transform
predicates in various ways, e.g., to apply a function over a value or to change
the operator of a predicate. Modifiers are basically the customization point to
enhance expressiveness of query operations.

The new [pySigma][pysigma] effort, which will eventually replace the
now-considered-legacy [sigma][sigma] project, comes with new modifiers as well.
Most notably, the `lt`, `lte`, `gt`, `gte` modifiers provide comparisons over
value domains with a total ordering, e.g., numbers: `x >= 42`. In addition, the
`cidr` modifier interprets a value as subnet, e.g., `10.0.0.0/8`. Richer typing!

[sigma]: https://github.com/SigmaHQ/sigma
[pysigma]: https://github.com/SigmaHQ/pySigma

Here's a real-world example of some modifiers:

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

[^1]: What happens under the hood is a padding a string with spaces. Anton
Kutepov's an [illustrative article][sigma-article] on how this works.

[sigma-article]: https://tech-en.netlify.app/articles/en513032/index.html

### Current status of modifier support

Our ultimate goal is to support a fully function executional platform for Sigma
rules. VAST is not quite there yet. The table below shows the current
implementation status of modifiers, where ✅ means implemented, 🚧 not yet
implemented but possible, and ❌ not yet supported by VAST's execution engine:

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
|`re`|interpret the value as regular expression|✅|❌
|`cidr`|interpret the value as a IP CIDR|❌|✅
|`all`|changes the expression logic from OR to AND|✅|✅
|`lt`|compare less than (`<`) the value|❌|✅
|`lte`|compare less than or equal to (`<=`) the value|❌|✅
|`gt`|compare greater than (`>`) the value|❌|✅
|`gte`|compare greater than or equal to (`>=`) the value|❌|✅
|`expand`|expand value to placeholder strings, e.g., `%something%`|❌|❌
