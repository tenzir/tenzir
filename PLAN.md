# Plan: Next `match` pattern extensions

This plan adds the next high-value extensions to TQL `match` after structural
record, range, binding, and arm-alternative patterns:

1. List patterns.
2. Extractor patterns.

The implementation should keep the current first-match-wins semantics and reuse
the pattern algebra introduced for enhanced `match` patterns.

## 1. List patterns

### Syntax

List patterns match list values structurally:

```tql
match tags {
  ["prod", $service, ..] => {
    environment = "production"
    service = $service
  }
  [] => {
    environment = "unknown"
  }
}
```

Nested list patterns work anywhere a pattern is accepted:

```tql
match this {
  {labels: ["prod", $service, ..], ..} => {
    service = $service
  }
}
```

### Semantics

- A list pattern is written as `[pattern, other, ..]`.
- Listed elements must exist and match in order.
- Without `..`, the list pattern is exact: extra elements prevent a match.
- With trailing `..`, extra trailing elements are ignored.
- `[]` matches only an empty list.
- `[..]` matches any list value.
- The first implementation should only allow `..` as the final list item.
  Prefix/suffix/middle rest such as `[.., last]` or `[first, .., last]` is out
  of scope.

### AST changes

Add a `list_pattern` kind:

```cpp
struct list_pattern {
  location begin;
  std::vector<Box<match_pattern>> elements;
  Option<location> rest;
  location end;
};
```

Extend `match_pattern_kind` with `list_pattern` and update inspection,
constructors, `get_location()`, and traversal.

### Parser changes

In `parse_match_pattern()`, parse `[` in pattern position as `list_pattern`, not
as a list expression.

Parsing rules:

- `[` starts a list pattern.
- Elements are parsed with `parse_match_pattern()`.
- Commas separate elements.
- `..` marks list rest.
- `..` must appear at most once and must be the final item.
- `]` ends the pattern.

Concrete parser shape:

```cpp
auto parse_list_pattern() -> ast::match_pattern {
  auto begin = expect(tk::lbracket);
  auto elements = std::vector<Box<ast::match_pattern>>{};
  auto rest = Option<location>{None{}};
  while (not peek(tk::rbracket)) {
    if (auto dots = accept(tk::dot_dot)) {
      if (rest.is_some()) { ... }
      rest = dots.location;
      if (not peek(tk::rbracket)) { ... }
      break;
    }
    elements.push_back(Box{parse_match_pattern()});
    if (not accept(tk::comma)) {
      break;
    }
  }
  auto end = expect(tk::rbracket);
  return ast::match_pattern{ast::list_pattern{begin.location,
                                              std::move(elements), rest,
                                              end.location}};
}
```

### Binding support

List bindings are addressable for list indices when the scrutinee path is
addressable:

```tql
match tags {
  ["prod", $service, ..] => {
    service = $service
  }
}
```

The binding expression for `$service` should be `tags[1]` when the scrutinee is
`tags`. For nested records, append both field and index path segments.

Implementation advice:

- Replace the current binding path representation of only identifiers with a
  small structural path variant:

```cpp
struct binding_field_path_segment { ast::identifier name; };
struct binding_index_path_segment { location source; int64_t index; };
using binding_path_segment
  = variant<binding_field_path_segment, binding_index_path_segment>;
using BindingPath = std::vector<binding_path_segment>;
```

- Extend `make_binding_expression()` to emit `ast::field_access` for field
  segments and `ast::index_expr` for index segments.
- Use constant integer expressions for index segments.
- Reject list-element bindings below non-addressable scrutinee expressions with
  the existing non-addressable binding diagnostic.

### Lowering and runtime matching

Extend the runtime `MatchPattern` tree:

```cpp
struct List {
  std::vector<Box<MatchPattern>> elements;
  bool has_rest = false;
};
```

Add `List` to the runtime variant.

Matching rules:

- The scrutinee value must be a list.
- Every listed element must exist and match the corresponding nested pattern.
- If `has_rest` is false, the list size must equal the number of listed
  elements.
- If `has_rest` is true, the list may have additional trailing elements.

Implementation advice:

- Match on `list_view3` directly, like records use `record_view3`.
- Avoid materializing full lists for every row.
- Use the list view iterator or `at(index)` for element lookup.

### Tests

Add integration tests:

- Exact list match.
- List with trailing `..`.
- `[]` matches only empty lists.
- `[..]` matches any list.
- Nested list pattern inside a record pattern.
- Bind a list element and use it in the arm pipeline.
- Missing element does not match.
- Wrong element value does not match.
- Extra element rejects exact list without `..`.
- Rest before another element is rejected.
- Duplicate list rest is rejected.
- List-element binding below non-addressable scrutinee is rejected.

## 2. Extractor patterns

Extractor patterns match strings with a parser-like extractor and expose
captures as bindings.

This is more ambitious than list patterns. Prefer implementing it after list
patterns have landed.

### Syntax

Use an explicit extractor introducer to avoid ambiguity with existing string,
pattern, and expression syntax.

Preferred first syntax:

```tql
match url {
  regex "https?://(?<host>[^/]+)" => {
    domain = $host
  }
}
```

Nested in records:

```tql
match this {
  {url: regex "https?://(?<host>[^/]+)", ..} => {
    domain = $host
  }
}
```

The `regex` introducer should be contextual in pattern position for the first
implementation. Avoid regex literal syntax for now unless TQL already adopts
one globally.

### Semantics

- The scrutinee value must be a string.
- The extractor succeeds when the regex matches.
- Named capture groups become bindings in the arm pipeline.
- Captures are strings.
- Missing optional captures bind to `null` or are rejected for the first
  implementation. Prefer rejecting optional captures initially to avoid nullable
  binding surprises.
- Unnamed capture groups do not create bindings.
- Capture names share the same namespace as pattern bindings and must be unique
  within an arm.
- If an outer `let` with the same name exists, reject the capture name as
  ambiguous. Do not reinterpret regex captures as constant patterns.

### AST changes

Add an extractor pattern kind:

```cpp
struct regex_pattern {
  location keyword;
  ast::expression regex;
  std::vector<identifier> captures;
};
```

The `captures` vector can be filled during binding/validation instead of parsing
if the regex expression is not yet substituted.

Extend `match_pattern_kind` with `regex_pattern` and update inspection,
constructors, `get_location()`, and traversal.

### Parser changes

In `parse_match_pattern()`, recognize contextual `regex` before falling back to
expression patterns:

```cpp
if (peek_identifier("regex")) {
  auto keyword = expect(tk::identifier).location;
  auto regex = parse_expression(1);
  return ast::match_pattern{ast::regex_pattern{keyword, std::move(regex), {}}};
}
```

The regex operand should be a constant expression after `let` substitution.

### Binding and validation

During AST-to-IR compilation:

1. Bind/substitute the regex expression.
2. Constant-evaluate the regex string.
3. Compile the regex once during `substitute(..., instantiate=true)`.
4. Extract named capture groups from the compiled regex.
5. Validate capture names:
   - Must be valid TQL identifiers after `$` prefixing.
   - Must not duplicate other bindings in the arm.
   - Must not shadow outer `let` bindings.

Implementation advice:

- Reuse the duplicate-binding machinery, but extend it so regex captures
  contribute binding names.
- Store capture metadata in the lowered runtime matcher, not in the AST after
  substitution.
- Prefer a small helper that returns capture names from the regex using the same
  regex engine Tenzir already uses for regex functions/operators.

### Runtime strategy

Extend runtime `MatchPattern`:

```cpp
struct Regex {
  compiled_regex regex;
  std::vector<std::string> capture_names;
};
```

Matching rules:

- The value must be a string.
- Regex match success makes the pattern match.
- Capture values must be available to the arm pipeline.

The main design challenge is capture materialization. Unlike record/list
bindings, regex captures are not addressable scrutinee paths. For the first
implementation, choose one of these strategies:

#### Preferred: materialized binding columns

- During match processing, when a regex pattern matches, materialize capture
  values into temporary columns for the rows routed to that arm.
- Compile the arm pipeline in a scope where `$capture` refers to those temporary
  columns.
- Drop temporary columns before emitting arm output unless the user explicitly
  assigns them.

This is the most correct long-term design, but touches pipeline input shaping.

#### Minimal first version: match-only extractor

- Implement regex extractor patterns as match predicates first.
- Reject named captures with a diagnostic:

```text
error: regex capture bindings are not supported yet
hint: use a guard with match_regex(...) and assign captures separately
```

This still gives users regex-based match arms without capture materialization.
Only choose this if materialized binding columns are too large for the first
extractor PR.

### Tests

If implementing full captures:

- Regex pattern matches a string.
- Regex pattern does not match a string.
- Regex pattern does not match non-string values.
- Named capture binds into the arm pipeline.
- Multiple captures bind into the arm pipeline.
- Captures do not leak to other arms or after the `match`.
- Duplicate capture names are rejected.
- Capture name conflicting with a pattern binding is rejected.
- Capture name conflicting with an outer `let` is rejected.
- Non-constant regex expression is rejected.
- Invalid regex emits a diagnostic at the regex expression.

If implementing match-only extractors first:

- Regex pattern matches a string.
- Regex pattern does not match a string.
- Regex pattern does not match non-string values.
- Non-constant regex expression is rejected.
- Invalid regex emits a diagnostic.
- Named captures are rejected with the explicit unsupported-captures diagnostic.

## Recommended implementation order

1. List patterns.
   - Straightforward extension of record patterns.
   - Requires extending binding paths with list indices.
2. Extractor patterns.
   - Most useful when captures work, but capture materialization is a larger
     runtime change.

Do not implement range variants in this sequence.
