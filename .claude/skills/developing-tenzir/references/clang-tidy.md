# clang-tidy Reference

Authoritative linting rules from `.clang-tidy`. Enable with
`-DTENZIR_ENABLE_CLANG_TIDY=ON` in CMake or run `clang-tidy` directly.

## Philosophy

Start with all checks enabled (`*`), then disable categories and individual
checks that don't apply. This ensures new checks are automatically enabled
when clang-tidy is updated.

```yaml
Checks: >
  *,
  -abseil-*,
  -altera-*,
  ...
```

## Header Filter

Only Tenzir code is analyzed—dependencies are excluded:

```yaml
HeaderFilterRegex: "/tenzir/"
```

## Disabled Categories

These entire check families are disabled:

| Category            | Reason                                            |
| ------------------- | ------------------------------------------------- |
| `abseil-*`          | Abseil library not used                           |
| `altera-*`          | FPGA-specific checks                              |
| `boost-*`           | Boost library not used                            |
| `fuchsia-*`         | Fuchsia OS conventions                            |
| `google-*`          | Google style differs from project style           |
| `hicpp-*`           | High Integrity C++ — too strict for this codebase |
| `llvm-header-guard` | Uses `#pragma once` instead                       |
| `llvmlibc-*`        | LLVM libc checks not applicable                   |

## Disabled Individual Checks

These specific checks are disabled with rationale:

### Overly Strict

| Check                                       | Reason                                      |
| ------------------------------------------- | ------------------------------------------- |
| `bugprone-easily-swappable-parameters`      | Too many false positives with similar types |
| `cppcoreguidelines-avoid-magic-numbers`     | Numeric literals often appropriate          |
| `readability-magic-numbers`                 | Same as above                               |
| `readability-identifier-length`             | Short names like `x`, `i` are fine          |
| `readability-function-cognitive-complexity` | Complexity limits too restrictive           |
| `misc-no-recursion`                         | Recursion is valid in many cases            |

### Style Preferences

| Check                                           | Reason                                        |
| ----------------------------------------------- | --------------------------------------------- |
| `modernize-use-trailing-return-type`            | Trailing return not required everywhere       |
| `modernize-use-nodiscard`                       | Don't auto-add `[[nodiscard]]`                |
| `modernize-use-designated-initializers`         | C++20 designated initializers optional        |
| `modernize-use-ranges`                          | Ranges not universally adopted                |
| `readability-braces-around-statements`          | clang-format handles this with `InsertBraces` |
| `readability-else-after-return`                 | Style preference, not enforced                |
| `readability-named-parameter`                   | Unnamed parameters acceptable                 |
| `readability-uppercase-literal-suffix`          | `1.0f` vs `1.0F` not enforced                 |
| `readability-redundant-inline-specifier`        | Keep explicit `inline` for clarity            |
| `readability-avoid-nested-conditional-operator` | Nested ternaries sometimes clear              |

### Technical Limitations

| Check                                       | Reason                                         |
| ------------------------------------------- | ---------------------------------------------- |
| `bugprone-lambda-function-name`             | `__FUNCTION__` in lambdas has defined behavior |
| `bugprone-suspicious-stringview-data-usage` | Too strict for string_view patterns            |
| `bugprone-switch-missing-default-case`      | Not all switches need default                  |
| `cert-dcl16-c`                              | C-style concerns not applicable                |
| `cert-err58-cpp`                            | Static init exceptions acceptable              |
| `performance-enum-size`                     | Enum sizing optimization not required          |

### C++ Core Guidelines Relaxations

| Check                                                    | Reason                                       |
| -------------------------------------------------------- | -------------------------------------------- |
| `cppcoreguidelines-avoid-const-or-ref-data-members`      | Const/ref members acceptable                 |
| `cppcoreguidelines-avoid-do-while`                       | Do-while loops are fine                      |
| `cppcoreguidelines-avoid-non-const-global-variables`     | Globals used judiciously                     |
| `cppcoreguidelines-avoid-reference-coroutine-parameters` | Coroutine ref params acceptable              |
| `cppcoreguidelines-macro-usage`                          | Macros used for logging, etc.                |
| `cppcoreguidelines-pro-bounds-pointer-arithmetic`        | Pointer arithmetic acceptable                |
| `cppcoreguidelines-pro-type-reinterpret-cast`            | `reinterpret_cast` needed for low-level code |
| `cppcoreguidelines-pro-type-static-cast-downcast`        | Static downcasts used carefully              |
| `cppcoreguidelines-pro-type-union-access`                | Union access acceptable                      |

## Check Options

Fine-tuning for specific checks:

```yaml
CheckOptions:
  - key: readability-implicit-bool-conversion.AllowPointerConditions
    value: true
  - key: misc-non-private-member-variables-in-classes.IgnoreClassesWithAllMemberVariablesBeingPublic
    value: true
  - key: cppcoreguidelines-special-member-functions.AllowSoleDefaultDtor
    value: true
  - key: cppcoreguidelines-avoid-do-while.IgnoreMacros
    value: true
```

### What These Allow

| Option                                                 | Effect                                             |
| ------------------------------------------------------ | -------------------------------------------------- |
| `AllowPointerConditions: true`                         | `if (ptr)` instead of `if (ptr != nullptr)`        |
| `IgnoreClassesWithAllMemberVariablesBeingPublic: true` | Public-only structs don't need private members     |
| `AllowSoleDefaultDtor: true`                           | `~Class() = default` without other special members |
| `IgnoreMacros: true`                                   | Do-while loops in macros are acceptable            |

## Key Enabled Checks

Important checks that remain enabled:

| Category              | Examples                                      |
| --------------------- | --------------------------------------------- |
| `bugprone-*`          | Use-after-move, forwarding reference overload |
| `cert-*`              | Security-focused checks                       |
| `clang-analyzer-*`    | Static analysis for memory issues             |
| `cppcoreguidelines-*` | Modern C++ practices (with exceptions above)  |
| `misc-*`              | Miscellaneous improvements                    |
| `modernize-*`         | C++11/14/17/20 modernization                  |
| `performance-*`       | Performance improvements                      |
| `readability-*`       | Code clarity (with exceptions above)          |

## Running clang-tidy

Build with clang-tidy enabled:

```sh
cmake -DTENZIR_ENABLE_CLANG_TIDY=ON ...
```

Or run manually on a file:

```sh
clang-tidy --config-file=.clang-tidy src/file.cpp
```

Warnings are not treated as errors by default:

```yaml
WarningsAsErrors: ""
```
