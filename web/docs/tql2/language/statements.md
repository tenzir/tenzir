---
sidebar_position: 1
---

# Statements

TQL programs are structured as a sequence of operators that perform various functions on data streams. Each operator can be thought of as a modular unit that processes data and can be combined with other operators to create complex workflows.

## Operator

Operator statements consist of the operator name, followed by an arbitrary number of arguments. Arguments are delimited by commas and may optionally be enclosed in parentheses. If the last argument is a pipeline expression, the preceding comma can be omitted for brevity.

Arguments can be specified in two ways: they can be positional, where the order matters, or named, where each argument is explicitly associated with a parameter name. Furthermore, arguments can be classified as required or optional. Some operators expect constant values, while others are designed to accept runtime values.

```tql
select foo, bar.baz
drop qux
head 42
sort abs(x)
```

## Assignment

An assignment statement in TQL is structured as `<place> = <expression>`, where `<place>` typically refers to a field or item of a list. If the specified place already exists, the assignment will overwrite its current value. If it does not exist, a new field will be created.

The `<place>` can also reference a field path. For example, the statement `foo.bar = 42` assigns the value 42 to the field `bar` within the record `foo`. If `foo` is not a record or does not exist before, it will be set to a record containing just the field `bar`.

```tql
category_name = "Network Activity"
type_uid = class_uid * 100 + activity_id
traffic.bytes_out = event.sent_bytes
```

## `if`

The `if` statement is a primitive designed to route data based on a predicate. Its typical usage follows the syntax `if <expression> { … } else { … }`, where two subpipelines are specified within the braces. When its expression evaluates to `true`, the first pipeline processes the event. Conversely, when it evaluates to `false`, it is routed through the second one.

After the `if` statement the event flow from both pipelines is joined together. The `else` clause can be omitted, resulting in the syntax `if <expression> { … }`, which has the same behavior as `if <expression> { … } else {}`. Additionally, the `else` keyword can be followed by another `if` statement, allowing for chained `if` statements. This chaining can be repeated, enabling complex conditional logic to be implemented.

```tql
if score < 100 {
  severity = "medium"
  drop details
} else {
  severity = "high"
}
```

## `let`

The `let` statement binds a constant to a specific name within the pipeline's scope. The syntax for a `let` statement is `let $<identifier> = <expression>`. For instance, `let $meaning = 42` creates a constant `$meaning` that holds the value 42.

More complex expressions can also be assigned, such as `let $start = now() - 1h`, which binds `$start` to a value representing one hour before the pipeline was started. Constants defined with `let` can be referenced in subsequent statements, including other `let` statements. For example, `let $end = $start + 30min` can be used to define `$end` depending on the value of `$start`.

```tql
let $meaning = 42
let $start = now() - 1h
let $end = $start + 30min
```
