# C++ Coding Conventions

Older code may deviate from these conventions. When doing minor edits, use the
style that is most consistent with the surrounding code. However, substantial new
code should follow the new conventions.

## Style Conventions

- **Const placement (east const)**: Place `const` after the type (`T const&`),
  not before (`const T&`).
- **Prefer `auto`**: Use `auto` to avoid type repetition and let the compiler
  infer types. Make conversions explicit when needed (e.g.,
  `auto x = int64_t{0}`). Always provide a trailing return type for functions,
  unless the function requires type deduction from the `return` statement. Use
  `auto` or `decltype(auto)` in that case.
- **Vertical whitespace**: You must not insert blank lines between statements
  inside a function. Use comments to separate logical blocks instead.
- **Logical operators**: Use the word form `not`, `and`, `or` instead of `!`,
  `&&`, `||`.

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

## Comments

- `FIXME:` for bugs, `TODO:` for improvements (colon preferred)
- Doxygen: `///` with Markdown. Do not use `@param`, `@returns`, `@pre`, `@post`.
  These tags are legacy patterns that should not be used in new code.
- Comments must either explain a non-obvious property or act as a heading

## Errors

- For logic errors, use `TENZIR_ASSERT`, `panic` or other functions that panic
- For user errors, use `failure_or<T>`, `Result<T, U>` or other types if needed
- Do not use exceptions, but catch library exceptions caused by user errors
- Program defensively with `TENZIR_ASSERT` whenever it's not obvious
- `TENZIR_ASSERT` is enabled in release builds, but don't put side effects there

## Other

- Use `class` for template parameters; `typename` only for dependent types
- Use `Box<T>` for a `std::unique_ptr<T>` that cannot be null
- Use `std::optional<Box<T>>` for a `std::unique_ptr<T>` that may be null
- Use the current year in the copyright notice when creating new files. Do not
  update the copyright notice on existing files.

## Unused Variables and Unreachable Code

- Use `TENZIR_UNUSED(arg...)` ONLY for unused function arguments.
  Do not use this macro for ignoring return values
- Use `std::ignore = func()` to ignore a functions return value.
- Use `TENZIR_TODO()` to mark a code path as to-be-implemented
- Use `TENZIR_UNREACHABLE()` to mark a code path as truly unreachable.
  Use this after switch statements that handle all cases.
