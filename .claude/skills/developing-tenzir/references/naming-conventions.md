# Naming Conventions

## Summary Table

| Element          | Convention   | Example                      |
| ---------------- | ------------ | ---------------------------- |
| Classes          | `snake_case` | `table_slice`, `record_type` |
| Structs          | `snake_case` | `actor_state`                |
| Functions        | `snake_case` | `make_table_slice()`         |
| Variables        | `snake_case` | `row_count`                  |
| Constants        | `snake_case` | `default_timeout`            |
| Template params  | `CamelCase`  | `typename T`, `class Actor`  |
| Macros           | `UPPER_CASE` | `PROJECT_ERROR`              |
| Member variables | `name_`      | `buffer_`, `state_`          |
| Namespaces       | `snake_case` | `namespace project`          |

## General Naming

All types, functions, and variables use lowercase with underscores:

```cpp
class table_slice {
  // ...
};

struct plugin_state {
  // ...
};

auto make_table_slice(const record& r) -> table_slice;

auto row_count = slice.rows();
```

### Template Parameters

Use CamelCase:

```cpp
template <class T>
struct my_trait;

template <class Actor, class... Handlers>
auto request(Actor& self, Handlers&&... handlers);
```

For generic unconstrained parameters, use `T` (or `Ts` for packs):

```cpp
template <class T, class... Ts>
auto f(T x, Ts... xs);
```

### Member Variables

Suffix with underscore. Getters/setters use the same name without suffix:

```cpp
class connection {
public:
  auto timeout() const -> duration {
    return timeout_;
  }

  void timeout(duration d) {
    timeout_ = d;
  }

private:
  duration timeout_;
  std::string name_;
};
```

Exception: Public members that constitute the struct's API don't need the
suffix:

```cpp
struct config {
  std::string name;   // Public API, no suffix
  int timeout;
};
```

## Naming Patterns

### Verbs vs Nouns

- **Types and variables**: Nouns (`table_slice`, `row_count`)
- **Action functions**: Verbs (`parse()`, `serialize()`, `connect()`)
- **Getters/setters**: Nouns without `get_`/`set_` prefix (`name()`, `timeout()`)
- **Metafunctions**: Verbs (`remove_const`, `decay`)

```cpp
// Good: verb for action
void serialize(const table_slice& slice);

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
auto compute_hash(const data& d) -> size_t {
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

class table_slice {
  // ...
};

} // namespace myproject
```

### Implementation Details

Put non-public implementation in `namespace detail`:

```cpp
namespace myproject::detail {

// Internal helper, not part of public API
auto parse_impl(std::string_view input) -> result;

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
friend constexpr auto operator<=>(const foo&, const foo&) = default;

// Bad: wrong order
constexpr static inline auto max_size = 1024;
```

## Consistency Guidelines

- Match existing patterns in the file/module
- Prefer clarity over brevity for public APIs
- Use abbreviations only when well-established (e.g., `num`, `ptr`, `impl`)
- Be consistent with standard library naming when wrapping or extending it
