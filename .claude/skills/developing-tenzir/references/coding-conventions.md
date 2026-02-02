# C++ Coding Conventions

## Style Conventions

- **Const placement (west const)**: Place `const` before the type (`const T&`),
  not after (`T const&`). This aids readability by making the const modifier
  prominent at the start of the type declaration. Template contexts may use
  east const for STL consistency.
- **Prefer `auto`**: Use `auto` to avoid type repetition and let the compiler
  infer types. Make conversions explicit when needed (e.g.,
  `auto x = int64_t{0}`). For functions, use `auto` with a trailing return type.
  Avoid `auto` for function return types in public APIs where the type should be
  explicit to users.
- **Vertical whitespace**: Avoid blank lines within functions. Use comments
  to separate logical blocks instead.
- **Logical operators**: Use the word form `not`, `and`, `or` instead of `!`,
  `&&`, `||`.
- **Naming**: See [Naming Conventions](./naming-conventions.md)

## File Organization

- Headers: `.hpp`, implementation: `.cpp`
- Forward declarations: `<module>/fwd.hpp`
- Use `#pragma once`—no manual include guards

## Classes

**Member order:**

1. `public`, then `protected`, then `private`
2. Within each: constructors, operators, mutating members, accessors

**Rules:**

- Mark single-argument constructors `explicit`
- Use `explicit(false)` when implicit conversion is intentional
- Follow the rule of zero or rule of five
- Declare move operations `noexcept`
- Use `struct` for simple data aggregates where the public members are the API

## Template Metaprogramming

- Use `class` for template parameters; `typename` only for dependent types
- Name parameters `T`, packs `Ts`, arguments `x`, packs `xs`
- Provide `*_t` and `*_v` helpers for traits

## Comments

- `FIXME:` for bugs, `TODO:` for improvements (colon preferred)
- Doxygen: `///` with Markdown—do not use `@param`, `@returns`, `@pre`, `@post`.
  These tags are legacy patterns that should not be used in new code.
