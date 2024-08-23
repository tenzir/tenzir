# Language

:::note
This is the reference.
:::

- Syntax?
- Line-joining with `\`
- Newline as separator + inside parenthesis
- Identifier rules?
- Comments
- Keywords
- Definition of other tokens

What form should this be in to be most helpful?

## Statements

- `let`
- operator invocation
- implicit `set`
- (`if` / `match`)


### Let

`let` followed by `$` identifier, `=` then expression

### Invocation

- operator name (not really)
- then arguments: expressions delimited by comma,
  optionally wrapped in parenthesis
- if last argument is pipeline expression, comma can be elided
- arguments can be positional or named, and required and optional
- named options: `<selector> = <expression>`
- some operators want constant values (what is that?)

### Implicit set

If the statement is of the form `<selector> = <expression>`
(explain: what is a selector?), then it is an assignment.
The semantics are the same as the `set` operator with a single
assignment. Prefer this over `set`.

### `if`

- `if <expression> { ... }`
- `if <expression> { ... } else { ... }`
- `if <expression> { ... } else <if_statement>`

### `match`

(not yet implemented)

## Expressions

- `{ foo: 42, bar: test() }` + `...`
- `[42, $okay]` + `...`
- `meta` / `@`
- `this`
- `my_field_name`: field
- `{ <pipeline> }`
- `42`: literal
- `<expr>.foo`
- `foo[bar]`
- `foo + bar`
- `-foo`
- `foo()` and `test.foo()`
- assignment ????
- `$identifier`
- `if foo { 1 } else { 2 }`

What is a selector?
