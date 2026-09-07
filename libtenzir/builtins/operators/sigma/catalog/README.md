# Sigma OCSF mapping catalog

This directory contains the built-in mapping catalog that lets the `sigma`
operator match stock Sigma rules against OCSF events. Each YAML file describes
one *mapping family*: how rules written for one Sigma logsource translate to
one OCSF event class. Files are organized as `<product>/<family>.yaml`.

The build embeds every `*.yaml` file into the binary via
`cmake/TenzirEmbedSigmaCatalog.cmake`; the parser in `../catalog.cpp` loads
them at startup and panics on any invalid document. The format is internal and
may change without notice.

## How it works

The operator translates the *rule*, not the data. A projection describes the
forward source-to-OCSF mapping whose effect the matcher reverses: when a rule
says `Image|endswith: '\cmd.exe'`, the operator evaluates that predicate
against `process.path` because the mapping declares that `process.path`
carries the meaning of the source field `Image`.

A translated rule is a statement about its OCSF event class, not about one
producer. A Zeek DNS rule matches conformant DNS Activity from any source.
Producer identity only matters for fields whose OCSF value lives in a
source-specific namespace (see `provenance-scoped` below).

A rule field that has no projection resolves literally against the schema. If
it does not exist there either, the whole rule is skipped with a warning — a
condition is never silently weakened.

## Document structure

```yaml
id: sigma/<product>/<family>     # unique catalog identifier
title: Human-readable name       # used by documentation and diagnostics

logsource:                       # which *rules* this family claims
  category: process_creation     # omit keys the rule must also omit
  product: windows
  service: sysmon
  optional-service: true         # also claim rules without a service

guard:                           # which *events* are semantically in scope
  class-uid: 1007                # the OCSF class this family targets
  activities: [1]                # the activity IDs this Sigma category means

fields:                          # sigma field -> OCSF projection
  <SigmaField>:
    primitive: <name>            # one of the core transformations below
    # ... primitive-specific parameters ...
    kind: string                 # optional schema-validation constraint
    rule-value: identity         # optional rule-side value transport
    provenance-scoped: false     # optional; see below
    notes: One sentence.         # optional; rendered in the documentation

unmapped:                        # fields normalization provably destroys
  - field: Z
    reason: The source-to-OCSF mapping does not preserve this field.
```

The parser is strict: unknown keys anywhere reject the document, and the unit
suite (`libtenzir/test/sigma_ocsf.cpp`) lints every document at test time.

### `logsource`: selecting rules

The selector reads the *rule*, never the event. It matches the exact
logsource triple; an omitted `category` or `service` requires the rule to
omit it too. Set `optional-service: true` when a service-specific family
should also claim the category-only form that many stock rules use.

Several documents may share one selector when they target distinct
`class-uid` values. Each such alternative plans independently, and disjoint
classes guarantee that at most one alternative matches a given event. Use
this for logsources that span multiple OCSF classes: the Windows Security
event log has one document per class (`windows/security_*.yaml`), the
Sysmon `registry_event` category splits into key and value activity, and
Okta system log events spread over six classes. A rule resolves against the
alternative whose projections cover its fields; a rule using only shared
fields such as `EventID` plans one variant per alternative.

When the source distinguishes sub-kinds that OCSF encodes in the class or
activity, project the source field onto `class_uid` or `activity_id` with a
`dictionary` rule value. Windows `ObjectType: File` becomes `class_uid ==
1001`, and Sysmon `EventType: DeleteKey` becomes `activity_id == 4`; a rule
naming a sub-kind the alternative does not represent evaluates to false there
and matches only in the alternative that does.

### `guard`: selecting events

Guards are conservative: they reject *known contradictions* and keep
missing, null, or unknown classifiers eligible, because real OCSF telemetry
is often sparse. The knobs:

- `class-uid` (required, nonzero): the event class this family targets.
- `activities` (required, non-empty): the activity IDs this Sigma category
  means, e.g. `[1]` for process creation because the category covers only
  the Launch activity. An event carrying any other specific activity
  declares itself to be something else and is rejected: a
  process-termination event never matches a process-creation rule. An
  absent activity, `0` (Unknown), and `99` (Other) always stay eligible and
  must not appear in the list.
- `source`, `log-name`, `conflicting-log-names`: producer expectations.
  These apply only to rules that use a `provenance-scoped` field; for all
  other rules they are inert. Source kinds use snake_case names such as
  `windows_security`. When two logsources share a source kind, name the
  channel and exclude the others: the Security documents declare
  `log-name: Security` with `conflicting-log-names: [System]`, and the
  System document the reverse, so a `7045` rule never compares against a
  Security-log event code.

One guard is derived, not declared: when the selector's `product` names an
operating system (`windows`, `linux`, or `macos`), events whose `device.os`
identifies a known other system are rejected. Unknown or absent OS
information stays eligible. Products such as `zeek` or `aws` imply no OS
constraint.

### `fields`: the projections

Every field references one primitive from the closed vocabulary implemented
in `../ocsf.cpp`. Extending the vocabulary is a deliberate engine change with
its own unit tests; prefer expressing a projection with the existing
primitives.

| Primitive     | Parameters                              | Meaning                                                        |
| ------------- | --------------------------------------- | -------------------------------------------------------------- |
| `literal`     | —                                       | Read the identically named event field.                        |
| `path`        | `path`                                  | Read one OCSF path.                                            |
| `fallback`    | `primary`, `fallback`                   | Read `primary`; use `fallback` when absent or empty.           |
| `join`        | `left`, `right`, `separator`, `evidence`| Rebuild the source value as `left + separator + right`.        |
| `principal`   | `domain`, `name`, `evidence`            | Rebuild `DOMAIN\name` from two paths.                          |
| `fingerprints`| `path`                                  | Rebuild an `ALGORITHM=value` list from OCSF Fingerprints.      |
| `list-member` | `path`, `member`                        | Read one member from every object in an OCSF list.             |

The `evidence` parameter names the OCSF path reported in finding observables
for reconstructed values.

Optional per-field keys:

- `kind`: validates the projected OCSF path against the concrete schema at
  plan time. One of `any` (default), `string`, `integral`, or
  `source-identifier` (string or integer; compared uniformly as strings).
- `rule-value`: transports rule-side constants into the projected
  representation:
  - `identity` (default): keep the value.
  - `stringify`: render non-strings as strings, e.g. `EventID: 1` against a
    string event code.
  - `dictionary`: translate names to integers via a lookup table with a
    `name` (for diagnostics) and lowercase `entries`. A rule value outside
    the table skips the rule.
- `provenance-scoped`: set to `true` when the OCSF value lives in a
  source-specific namespace, such as Sysmon event IDs in
  `metadata.event_code` or Zeek residue under `unmapped`. A rule using such
  a field gets the guard's producer constraints so that, for example,
  Sysmon event 13 never compares against a Windows Security event code.
  Leave unset for fields whose meaning is producer-independent. OCSF's
  free-form `data` objects, such as `entity.data` or `application.data`,
  are producer payloads by definition: a member name like `action_id` means
  something different for every source that writes one. Every projection
  that reads a member of a `data` object must therefore be
  provenance-scoped, and the catalog lint rejects one that is not. A leaf
  attribute that merely happens to be called `data`, such as
  `reg_value.data`, is a real OCSF attribute and needs no scope.
- `notes`: one sentence of matching behavior for the generated
  documentation. State only what the primitive's semantics do not already
  imply.

### `unmapped`: the honest boundary

List source fields that the OCSF mapping does not preserve, with a reason.
Rules using them are skipped for OCSF schemas; the list feeds diagnostics and
the documentation. An unmapped field must not also have a projection.

Projections name real OCSF attributes only. The operator never reads source
residue under OCSF's `unmapped` object, even when a normalizer preserved the
field there: residue is producer-specific by construction, and matching on it
would turn the translated rule back into a statement about one producer. When
a stock rule needs a field that only survives as residue, the fix belongs in
the normalizer, which should carry the value on a real OCSF path. Until then
the field is declared here as unmapped so that the rule skips visibly.

The catalog targets OCSF 1.9. Do not add fallbacks for paths or activity IDs
that only older schema versions define.

## Adding a mapping family

1. Add `<product>/<family>.yaml` here. The build picks it up on reconfigure;
   no C++ changes. Take the OCSF paths from the normalizer that produces the
   class, not from the schema alone: the projection inverts what the
   normalizer writes.
2. Add a fixture directory under
   `test/tests/operators/sigma/inputs/families/<name>/` with TQL-typed OCSF
   sample events (`events.tql`), representative stock-shaped rules
   (`rules/*.yml`), and a three-line driver test next to the existing ones.
   Give every guard clause a commented near-miss negative event and every
   projection an event that exercises it.
3. Run the unit suite; the catalog lint validates the document.
4. Check corpus coverage against a SigmaHQ checkout:
   `scripts/sigma-ocsf-coverage.py <sigma>/rules`. The report names the
   rules the family claims and the fields that still resolve to nothing.
5. Regenerate the documentation data:
   `scripts/generate-sigma-ocsf-catalog.py -o <docs>/src/data/sigma-ocsf-catalog.json`.
