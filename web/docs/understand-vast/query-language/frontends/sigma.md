# Sigma

The Sigma query frontend makes it possible to execute [Sigma
rules](https://github.com/SigmaHQ/sigma) in VAST. This means you can
provide a Sigma rule instead of a VASTQL expression when querying data. For
example:

```bash
vast export json < sigma-rule.yaml
```

Sigma defines a [YAML-based rule language][sigma-spec] along with a compiler
that transforms rules into the native query languages of SIEM systems. The
repository also ships with collection of detection rules that apply to endpoint
and network log telemetry.

## Usage

To use the Sigma frontend, [install the `sigma`
plugin](/docs/setup-vast/configure#plugins). Then use the `vast export` command
to provide a Sigma rule on standard input:

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

### Expressiveness

The majority of rule definitions include combinations of exact string lookups,
substring searches, or pattern matches. Sigma uses
[modifiers](https://github.com/SigmaHQ/sigma/wiki/Specification#value-modifiers)
to select a concrete operator for given search predicate. Without a modifier
specification, Sigma uses equality comparison (`==`) of field and value. For
example, the `contains` modifier changes the operator to substring search, and
the `re` modifier switches to a regular expression match.

Sigma currently lacks support for ordering relationships, such as less-than
comparison of numerical values, e.g., `x < 42` or `timestamp >= 2021-02`,
whereas VAST offers relational operators (`<`, `<=`, `>=`, `>`) for numeric
types.

## Compatibility

VAST's support for Sigma is still in the early stages and does not support the
full [language specification][sigma-spec]. The following features are currently
unsupported:

- Certain value invariants:
  - VAST does not offer case-insensitive search, whereas it's the default in
    Sigma
  - Interpretation of string values containing `*` and `?` wildcards  
- The following modifiers:
  - `re`
  - `base64`
  - `base64offset`
  - `utf16le` / `wide`
  - `utf16be`
  - `utf16`
- TimeFrame specification
- Aggregation expressions
- Near aggregation expressions

### Focus on Endpoint

Sigma predominantly offers rules with a focus on endpoint data, such as
[Sysmon](https://docs.microsoft.com/en-us/sysinternals/downloads/sysmon)
telemetry. While there exist rules for network-based detections (e.g., for
DNS queries, SMB events, and Kerberos traffic), they receive less attention.

As of Februrary 2021, the [rules][sigma-rules-2021-02] directory includes a
total of 709 total `*.yml` files compared to 36 files in the `network`
directory:

```bash
find rules -name '*.yml' | wc -l
709
find rules/network -name '*.yml' | wc -l
36
```

That is, network-based rules account only for **5%** of the total rules. This
illustrates the emphasis of the community and project authors, who have strong
background in endpoint detection.

VAST's history emphasizes network telemetry, with native support for PCAP,
NetFlow, and full support for network monitors like Zeek and Suricata. By
natively supporting Sigma in VAST, we are looking forward to offer a platform
with detection capabilities on both ends of the spectrum.

[sigma-spec]: https://github.com/SigmaHQ/sigma/wiki/Specification
[sigma-rules-2021-02]: https://github.com/SigmaHQ/sigma/tree/8ae8c213a97786e4e76e3094a50cd159498662f3/rules
