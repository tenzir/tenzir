# Sigma

The Sigma query frontend makes it possible to execute [Sigma
rules](https://github.com/SigmaHQ/sigma) in VAST. This means you can provide a
Sigma rule instead of a [VAST expression](../expressions.md) when querying data.

For example:

```bash
vast export json < sigma-rule.yaml
```

Sigma defines a [YAML-based rule language][sigma-spec] along with a compiler
that transforms rules into the native query languages of SIEM systems. VAST
takes a different approach and compiles the Sigma query directly into a native
query expression, without going through the Python tooling provided by the
SigmaHQ project. This has numerous advantages in exploiting the richer type
system of VAST. The translation process looks as follows:

![Sigma Query Frontend](sigma-query-frontend.excalidraw.svg)

[sigma-spec]: https://sigmahq.github.io/sigma-specification/

## Usage

Use the `vast export` command to provide a Sigma rule on standard input:

```bash
vast export <format> < sigma-rule.yaml
```

The `<format>` placeholder represents an output format, such as `json` or `csv`,
and `sigma-rule.yaml` a file containing a Sigma rule.

### Search Identifiers

The Sigma rule YAML format requires a `detection` attribute that includes a map
of named sub-expression called *search identifiers*. In addition, `detection`
must include a final `condition` that combines search identifiers using boolean
algebra (AND, OR, and NOT) or syntactic sugar to reference groups of search
expressions, e.g., using the `1/all of *` or plain wildcard syntax.

Consider the following Sigma `detection` embedded in a rule:

```yaml
detection:
  foo:
    a: 42
    b: "evil"
  bar:
    c: 1.2.3.4
  condition: foo or not bar
```

VAST translates this rule piece by building a symbol table of all keys (`foo`
and `bar`). Each sub-expression is a valid VAST expression itself:

1. `foo`: `a == 42 && b == "evil"`
2. `bar`: `c == 1.2.3.4`

Finally, VAST combines the expression according to the `condition`:

```c
(a == 42 && b == "evil") || ! (c == 1.2.3.4)
```

:::note Rich YAML Typing
Because VAST has a beefed up YAML parser that performs type inference, the YAML
snippet `c: 1.2.3.4` is parsed as a key-value pair with types `string` and
`address`. This means that we get the rich type system of VAST for free.
:::

### Taxonomy

Sigma comes with a [taxonomy](https://github.com/SigmaHQ/sigma/wiki/Taxonomy) to
facilitate rule sharing by standardizing field names of the supported data
sources.

:::caution Missing Definitions
VAST currently does not ship with a taxonomy to transparently map the canonical
Sigma fields to an equivalent in VAST. We will ship the missing mappings in the
future. To date, you must either use VAST concepts to re-implement the mappings
or wait until we have provided them.
:::

## Comparison

VAST and Sigma have many commonalities. They both support flexible construction
of search expressions using boolean algebra (AND, OR, NOT) and offer multiple
ways to define predicates and sub-expression. But there also exist differences
in expressiveness and intent. This section compares the two systems.

## Expressiveness

The majority of rule definitions include combinations of exact string lookups,
substring searches, or pattern matches. Sigma uses
[modifiers](https://github.com/SigmaHQ/sigma/wiki/Specification#value-modifiers)
to select a concrete operator for given search predicate. Without a modifier
specification, Sigma uses equality comparison (`==`) of field and value. For
example, the `contains` modifier changes the operator to substring search, and
the `re` modifier switches to a regular expression match. The now "legacy" sigma
compiler lacks support for ordering relationships, such as less-than comparison
of numerical values, e.g., `x < 42` or `timestamp >= 2021-02`. The
[pySigma](https://github.com/SigmaHQ/pySigma) project addresses this with the
additional modifiers `lt`, `lte`, `gt`, `gte`.

## Compatibility

VAST's support for Sigma is still in the early stages and does not support the
full [language specification][sigma-spec]. Most notable, UTF16 support is not
yet implemented.

The table below shows the current implementation status of modifiers, where ✅
means implemented, 🚧 not yet implemented but possible, and ❌ not yet supported
by VAST's query engine:

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
|`re`|interpret the value as regular expression|✅|✅
|`cidr`|interpret the value as a IP CIDR|❌|✅
|`all`|changes the expression logic from OR to AND|✅|✅
|`lt`|compare less than (`<`) the value|❌|✅
|`lte`|compare less than or equal to (`<=`) the value|❌|✅
|`gt`|compare greater than (`>`) the value|❌|✅
|`gte`|compare greater than or equal to (`>=`) the value|❌|✅
|`expand`|expand value to placeholder strings, e.g., `%something%`|❌|❌
