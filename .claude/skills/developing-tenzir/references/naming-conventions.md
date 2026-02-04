# Naming Conventions

## Summary Table

| Element              | Convention   | Example                        |
| -------------------- | ------------ | ------------------------------ |
| Classes              | `PascalCase` | `TableSlice`, `RecordType`     |
| Structs              | `PascalCase` | `ActorState`                   |
| Type aliases         | `PascalCase` | `ValueType`, `Iterator`        |
| Type traits          | `PascalCase` | `RemoveConst`, `IsHashable`    |
| Concepts             | `PascalCase` | `Hashable`, `Serializable`     |
| Functions            | `snake_case` | `make_table_slice()`           |
| Variables            | `snake_case` | `row_count`                    |
| Constants            | `snake_case` | `default_timeout`              |
| Enum members         | `snake_case` | `Color::red`, `Status::ok`     |
| Template params      | `PascalCase` | `typename T`, `class Actor`    |
| Macros               | `UPPER_CASE` | `PROJECT_ERROR`                |
| Member variables     | `name_`      | `buffer_`, `state_`            |
| Namespaces           | `snake_case` | `namespace project`            |
| STL type aliases     | `snake_case` | `value_type`, `iterator`       |

## Types

All types use PascalCase:

```cpp
class TableSlice {
  // ...
};

struct PluginState {
  // ...
};

using RecordBatch = arrow::RecordBatch;
```

For multi-letter abbreviations, only capitalize the first letter: `Abc`.
Consecutive upper letters are allowed if they are not part of the same
abbreviation: `VTable`.

### Type Aliases

Member type aliases use PascalCase:

```cpp
class Container {
public:
  using ValueType = int;
  using Pointer = ValueType*;
  using Reference = ValueType&;
};
```

**Exception**: STL-required type aliases use `snake_case` to maintain
compatibility with standard library algorithms and concepts:

```cpp
class MyIterator {
public:
  // STL-required aliases stay snake_case
  using value_type = int;
  using difference_type = std::ptrdiff_t;
  using pointer = value_type*;
  using reference = value_type&;
  using iterator_category = std::forward_iterator_tag;

  // Non-STL aliases use PascalCase
  using UnderlyingContainer = std::vector<int>;
};
```

### Type Traits and Metafunctions

Type traits and metafunctions use PascalCase:

```cpp
template <class T>
struct RemoveConstImpl {
  using Type = T;
};

template <class T>
struct RemoveConstImpl<const T> {
  using Type = T;
};

template <class T>
using RemoveConst = typename RemoveConstImpl<T>::Type;

template <class T>
struct IsHashable : std::false_type {};
```

### Concepts

Concepts use PascalCase:

```cpp
template <class T>
concept Hashable = requires(T x) {
  { std::hash<T>{}(x) } -> std::convertible_to<std::size_t>;
};

template <class T>
concept Serializable = requires(T x, Serializer& s) {
  { x.serialize(s) } -> std::same_as<void>;
};
```

## Functions and Variables

Functions and variables use lowercase with underscores:

```cpp
auto make_table_slice(const Record& r) -> TableSlice;

auto row_count = slice.rows();
```

### Enum Members

Enum members use `snake_case`:

```cpp
enum class Color {
  red,
  green,
  blue,
};

auto c = Color::red;
```

### Template Parameters

Use PascalCase:

```cpp
template <class T>
struct MyTrait;

template <class Actor, class... Handlers>
auto request(Actor& self, Handlers&&... handlers);
```

For generic unconstrained parameters, use single-letter names (`T` for types,
`x`/`xs` for values):

```cpp
template <class T, class... Ts>
auto f(T x, Ts... xs);
```

For semantically specific parameters, use descriptive names even in generic
code:

```cpp
template <class Actor>
void handle(Actor& actor);

template <class Handler, class ErrorHandler>
auto with_retry(Handler on_success, ErrorHandler on_error);
```

### Member Variables

Member variable naming depends on whether the type is an encapsulated class or a
public data aggregate.

#### Encapsulated Classes

Private members use an underscore suffix. Getters and setters use the same name
without suffix. For setters, prefer descriptive parameter names that avoid
shadowing:

```cpp
class Connection {
public:
  auto timeout() const -> Duration {
    return timeout_;
  }

  void timeout(Duration new_timeout) {
    timeout_ = new_timeout;
  }

  void name(std::string_view new_name) {
    name_ = new_name;
  }

private:
  Duration timeout_;
  std::string name_;
};
```

#### Public Data Aggregates

Structs with all-public members that serve as transparent data containers do not
use the underscore suffix. These types expose their members directly as part of
their API:

```cpp
struct Config {
  std::string name;
  int timeout;
};
```

## Naming Patterns

### Verbs vs Nouns

- **Types and variables**: Nouns (`TableSlice`, `row_count`)
- **Action functions**: Verbs (`parse()`, `serialize()`, `connect()`)
- **Getters/setters**: Nouns without `get_`/`set_` prefix (`name()`, `timeout()`)

```cpp
// Good: verb for action
void serialize(const TableSlice& slice);

// Good: noun for getter, no get_ prefix
auto name() const -> std::string_view;

// Bad: unnecessary get_ prefix
auto get_name() const -> std::string_view;
```

### Generic Temporaries

Use `x`, `y`, `z` for generic variables. Use `xs`, `ys`, `zs` for collections:

```cpp
template <class T>
auto transform(T x) {
  return process(x);
}

for (auto& x : xs) {
  handle(x);
}
```

### Result Variables

Name return values `result`:

```cpp
auto compute_hash(const Data& d) -> size_t {
  auto result = size_t{0};
  // ... computation ...
  return result;
}
```

## Namespaces

### Project Namespace

All code lives in a project namespace:

```cpp
namespace myproject {

class TableSlice {
  // ...
};

} // namespace myproject
```

### Implementation Details

Put non-public implementation in `namespace detail`:

```cpp
namespace myproject::detail {

// Internal helper, not part of public API
auto parse_impl(std::string_view input) -> Result;

} // namespace myproject::detail
```

### Static Non-const Variables

Put in anonymous namespace:

```cpp
namespace {

std::atomic<int> instance_count{0};

} // namespace
```

## Macros

Prefix with a project identifier to avoid clashes:

```cpp
#define MYPROJECT_ERROR(...) /* ... */
#define MYPROJECT_DEBUG(...) /* ... */
#define MYPROJECT_ASSERT(x)  /* ... */
```

## Declaration Specifier Order

When declaring variables and functions, order specifiers as follows:

1. Storage class: `static`, `thread_local`, `mutable`, `extern`
2. Then in order: `friend`, `inline`, `virtual`, `explicit`, `constexpr`,
   `consteval`, `constinit`

```cpp
// Good
static inline constexpr auto max_size = 1024;
friend constexpr auto operator<=>(const Foo&, const Foo&) = default;

// Bad: wrong order
constexpr static inline auto max_size = 1024;
```

## Consistency Guidelines

- Match existing patterns in the file/module
- Prefer clarity over brevity for public APIs
- Use abbreviations only when well-established (e.g., `num`, `ptr`, `impl`)
- Be consistent with standard library naming when wrapping or extending it
