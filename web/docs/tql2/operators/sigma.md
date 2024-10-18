# sigma

Filter the input with [Sigma rules][sigma] and output matching events.

[sigma]: https://github.com/SigmaHQ/sigma

```tql
sigma path:str, [refresh_interval=duration]
```

## Description

The `sigma` operator executes [Sigma rules](https://github.com/SigmaHQ/sigma) on
its input. If a rule matches, the operator emits a `tenzir.sigma` event that
wraps the input record into a new record along with the matching rule. The
operator discards all events that do not match the provided rules.

<details>
<summary> Transpilation Process </summary>

For each rule, the operator transpiles the YAML into an
[expression](../language/expressions.md) and instantiates a
[`where`](where.md) operator, followed by assignments to generate an output.
Here's how the transpilation works. The Sigma rule YAML format requires a
`detection` attribute that includes a map of named sub-expression called *search
identifiers*. In addition, `detection` must include a final `condition` that
combines search identifiers using boolean algebra (AND, OR, and NOT) or
syntactic sugar to reference groups of search expressions, e.g., using the
`1/all of *` or plain wildcard syntax. Consider the following `detection`
embedded in a rule:

```yaml
detection:
  foo:
    a: 42
    b: "evil"
  bar:
    c: 1.2.3.4
  condition: foo or not bar
```

We translate this rule piece by building a symbol table of all keys (`foo` and
`bar`). Each sub-expression is a valid expression in itself:

1. `foo`: `a == 42 && b == "evil"`
2. `bar`: `c == 1.2.3.4`

Finally, we combine the expression according to `condition`:

```tql
(a == 42 && b == "evil") || ! (c == 1.2.3.4)
```

We parse the YAML string values according to Tenzir's richer data model, e.g.,
the expression `c: 1.2.3.4` becomes a field named `c` and value `1.2.3.4` of
type `ip`, rather than a `string`. Sigma also comes with its own [event
taxonomy](https://github.com/SigmaHQ/sigma-specification/blob/main/Taxonomy_specification.md)
to standardize field names. The `sigma` operator currently does not normalize
fields according to this taxonomy but rather takes the field names verbatim from
the search identifier.

</details>

Sigma uses [value
modifiers](https://github.com/SigmaHQ/sigma-specification/blob/main/Sigma_specification.md#value-modifiers)
to select a concrete relational operator for given search predicate. Without a
modifier, Sigma uses equality comparison (`==`) of field and value. For example,
the `contains` modifier changes the relational operator to substring search, and
the `re` modifier switches to a regular expression match. The table below shows
what modifiers the `sigma` operator supports, where ✅ means implemented, 🚧 not
yet implemented but possible, and ❌ not yet supported:

|Modifier|Use|sigmac|Tenzir|
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

### `path: str`

The rule to match.

If `path` points to a rule, the operator transpiles the rule file at the time of pipeline creation.

If this points to a directory, the operator watches it and attempts to parse each contained file as
a Sigma rule. The `sigma` operator matches if *any* of the contained rules
match, effectively creating a disjunction of all rules inside the directory.

### `refresh_interval = duration (optional)`

How often the `sigma` operator looks at the specified rule or directory of rules
to update its internal state.

Defaults to `5s`.

## Examples

Apply a Sigma rule to an EVTX file using
[`evtx_dump`](https://github.com/omerbenamram/evtx):

```bash
evtx_dump -o jsonl file.evtx | tenzir --tql2 'read_json | sigma "rule.yaml"'
```

Apply a Sigma rule over historical data in a node from the last day:

```tql
export
where ts > now() - 1d
sigma "rule.yaml"
```

Watch a directory of Sigma rules and apply all of them on a continuous stream of
Suricata events:

```tql
load_file "eve.json", follow=true
read_suricata
sigma "/tmp/rules/"
```

When you add a new file to `/tmp/rules`, the `sigma` operator transpiles it and
will match it on all subsequent inputs.
