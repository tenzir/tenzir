# C++ Coding Conventions

## Tooling

Use clang-format for formatting and clang-tidy for linting. The `.clang-format`
and `.clang-tidy` files in the repository root are authoritative—run the tools
and trust the output.

## Style Conventions

These are not enforced by tooling:

- **West const**: `const T&` not `T const&`
- **Prefer `auto`**: Use almost-always-auto, make conversions explicit (e.g., `auto x = int64_t{0}`)
- **Vertical whitespace**: Avoid blank lines within functions. Use comments
  to separate logical blocks instead.
- **Naming**: See [naming-conventions.md](./naming-conventions.md)

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

- `FIXME:` for bugs, `TODO:` for improvements (colon required)
- Doxygen: `///` with Markdown—do not use `@param`, `@returns`, `@pre`, `@post`
