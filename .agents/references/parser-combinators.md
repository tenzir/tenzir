# Parser Combinators

Parser combinator framework in `tenzir/concept/parseable/`.

## Usage

```cpp
#include <tenzir/concept/parseable/core.hpp>
#include <tenzir/concept/parseable/numeric.hpp>

int value;
if (parsers::i32("42", value)) { /* value == 42 */ }

// With iterators (advances on success)
auto it = input.begin();
if (parser(it, input.end(), result)) { ... }

// Get optional result
auto maybe = parser.apply(it, end);  // Returns std::optional<attribute>
```

## Operators

| Op        | Name     | Description                     |
| --------- | -------- | ------------------------------- |
| `a >> b`  | Sequence | Parse `a` then `b`              |
| `a \| b`  | Choice   | Try `a`, on failure try `b`     |
| `*p`      | Kleene   | Zero or more repetitions        |
| `+p`      | Plus     | One or more repetitions         |
| `-p`      | Optional | Zero or one                     |
| `&p`      | And      | Positive lookahead (no consume) |
| `!p`      | Not      | Negative lookahead (no consume) |
| `p % sep` | List     | `p` separated by `sep`          |

## Built-in Parsers

```cpp
// Numeric — concept/parseable/numeric.hpp
parsers::i8, i16, i32, i64        // Signed integers
parsers::u8, u16, u32, u64        // Unsigned integers
parsers::real                     // Floating point
parsers::boolean                  // true/false

// String/char — concept/parseable/string.hpp
parsers::chr{'x'}                 // Single character
parsers::lit{"foo"}               // Literal string
parsers::any                      // Any single character
parsers::eoi                      // End of input

// Character classes — concept/parseable/string/char_class.hpp
parsers::digit, alpha, alnum, space, xdigit, upper, lower, printable
```

## Semantic Actions

```cpp
// Transform result
auto doubled = parsers::i32.then([](int x) { return x * 2; });
auto doubled = parsers::i32 ->* [](int x) { return x * 2; };  // Shorthand

// Guard: only succeed if predicate holds
auto positive = parsers::i32.with([](int x) { return x > 0; });

// Ignore result with _p literal
using namespace parser_literals;
auto p = "prefix:"_p >> parsers::i32;  // Captures only the int
```

## Key Headers

- `tenzir/concept/parseable/core.hpp` — All combinators
- `tenzir/concept/parseable/numeric.hpp` — Integer/real parsers
- `tenzir/concept/parseable/string.hpp` — String/char parsers
- `tenzir/concept/parseable/core/parser.hpp` — `parser_base`, `parser_registry`
