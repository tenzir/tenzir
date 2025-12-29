# clang-format Reference

Authoritative formatting rules from `.clang-format`. Run `clang-format` to
auto-format code or let the formatter hook handle it on save.

## Core Settings

| Setting               | Value   | Effect                  |
| --------------------- | ------- | ----------------------- |
| `IndentWidth`         | `2`     | 2-space indentation     |
| `UseTab`              | `Never` | Spaces only, no tabs    |
| `Standard`            | `Cpp11` | C++11 syntax baseline   |
| `PointerAlignment`    | `Left`  | `T* ptr` not `T *ptr`   |
| `MaxEmptyLinesToKeep` | `1`     | No multiple blank lines |
| `InsertNewlineAtEOF`  | `true`  | Ensure trailing newline |
| `KeepEmptyLinesAtEOF` | `false` | No trailing blank lines |

## Braces and Blocks

| Setting                               | Value    | Effect                                |
| ------------------------------------- | -------- | ------------------------------------- |
| `BreakBeforeBraces`                   | `Attach` | Opening brace on same line            |
| `InsertBraces`                        | `true`   | Add braces to single-statement blocks |
| `AllowShortBlocksOnASingleLine`       | `false`  | No `{ return x; }` on one line        |
| `AllowShortFunctionsOnASingleLine`    | `None`   | Functions always multi-line           |
| `AllowShortIfStatementsOnASingleLine` | `false`  | If statements multi-line              |
| `AllowShortLoopsOnASingleLine`        | `false`  | Loops multi-line                      |
| `AllowShortLambdasOnASingleLine`      | `Empty`  | Only empty lambdas `[]{}` inline      |
| `AllowShortCaseLabelsOnASingleLine`   | `false`  | Case labels multi-line                |

## Spacing

| Setting                        | Value               | Effect                  |
| ------------------------------ | ------------------- | ----------------------- |
| `SpaceBeforeParens`            | `ControlStatements` | `if (x)` but `f(x)`     |
| `SpaceAfterLogicalNot`         | `true`              | `if (! x)` with space   |
| `SpaceAfterTemplateKeyword`    | `true`              | `template <class T>`    |
| `SpaceAfterCStyleCast`         | `false`             | `(int)x` no space       |
| `SpaceBeforeCpp11BracedList`   | `false`             | `T{x}` no space         |
| `SpacesInAngles`               | `false`             | `<T>` not `< T >`       |
| `SpacesInParentheses`          | `false`             | `(x)` not `( x )`       |
| `SpacesInContainerLiterals`    | `false`             | `{1, 2}` not `{ 1, 2 }` |
| `SpacesBeforeTrailingComments` | `1`                 | One space before `//`   |

## Line Breaking

| Setting                                     | Value         | Effect                              |
| ------------------------------------------- | ------------- | ----------------------------------- |
| `BreakBeforeBinaryOperators`                | `All`         | Break before `&&`, `+`, etc.        |
| `BreakBeforeTernaryOperators`               | `true`        | Break before `?` and `:`            |
| `AlwaysBreakTemplateDeclarations`           | `Yes`         | `template` on its own line          |
| `BreakConstructorInitializers`              | `BeforeColon` | Colon on new line                   |
| `BreakInheritanceList`                      | `BeforeColon` | Inheritance colon on new line       |
| `AlwaysBreakAfterReturnType`                | `None`        | Return type on same line            |
| `BinPackArguments`                          | `true`        | Pack arguments when possible        |
| `BinPackParameters`                         | `true`        | Pack parameters when possible       |
| `AllowAllParametersOfDeclarationOnNextLine` | `false`       | Don't allow all params on next line |

## Indentation

| Setting                             | Value       | Effect                           |
| ----------------------------------- | ----------- | -------------------------------- |
| `AccessModifierOffset`              | `-2`        | Access modifiers outdented       |
| `IndentCaseLabels`                  | `true`      | Indent case labels               |
| `IndentPPDirectives`                | `AfterHash` | `#  include` indented            |
| `IndentRequires`                    | `true`      | Indent requires clauses          |
| `IndentWrappedFunctionNames`        | `false`     | Don't extra-indent wrapped names |
| `ConstructorInitializerIndentWidth` | `2`         | 2-space for initializers         |
| `ContinuationIndentWidth`           | `2`         | 2-space continuation             |
| `NamespaceIndentation`              | `None`      | Don't indent namespace contents  |

## Include Sorting

Includes are automatically sorted and grouped:

```yaml
IncludeBlocks: Regroup
SortIncludes: true
IncludeCategories:
  - Regex: '^"[[:alnum:]_.]+/fwd.hpp"$'
    Priority: 1 # Forward declarations first
  - Regex: '^"[[:alnum:]_.]+/'
    Priority: 2 # Local project headers
  - Regex: "^<tenzir/"
    Priority: 3 # Tenzir system headers
  - Regex: "^<[[:alnum:]_.]+/"
    Priority: 4 # Third-party headers
  - Regex: "^<[[:alnum:]_.]+>$"
    Priority: 5 # Standard library
```

Example result:

```cpp
#include "tenzir/fwd.hpp"           // Priority 1

#include "tenzir/detail/helper.hpp" // Priority 2

#include <tenzir/table_slice.hpp>   // Priority 3

#include <caf/actor.hpp>            // Priority 4

#include <memory>                   // Priority 5
#include <string>
```

## Special Macros

CAF framework macros are recognized as block delimiters:

```yaml
MacroBlockBegin: "CAF_BEGIN_TYPE_ID_BLOCK"
MacroBlockEnd: "CAF_END_TYPE_ID_BLOCK"
```

## Penalties

These influence where clang-format chooses to break:

| Setting                                | Value | Effect                               |
| -------------------------------------- | ----- | ------------------------------------ |
| `PenaltyBreakAssignment`               | `0`   | Free to break after `=`              |
| `PenaltyBreakBeforeFirstCallParameter` | `30`  | Prefer not breaking before first arg |
| `PenaltyBreakString`                   | `80`  | String breaks are costly             |
| `PenaltyExcessCharacter`               | `100` | Over-long lines very costly          |
| `PenaltyReturnTypeOnItsOwnLine`        | `5`   | Slight penalty for return type break |

## Other Settings

| Setting                            | Value   | Effect                                    |
| ---------------------------------- | ------- | ----------------------------------------- |
| `Cpp11BracedListStyle`             | `true`  | Modern brace init formatting              |
| `FixNamespaceComments`             | `true`  | Add `// namespace foo` comments           |
| `ReflowComments`                   | `true`  | Wrap long comments                        |
| `SortUsingDeclarations`            | `true`  | Sort `using` declarations                 |
| `CompactNamespaces`                | `false` | Don't compact `namespace a { namespace b` |
| `KeepEmptyLinesAtTheStartOfBlocks` | `false` | No blank line after `{`                   |
