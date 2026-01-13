# Common Types

Reusable common types from `include/tenzir/`.

## Generator — `tenzir/generator.hpp`

Lazy sequence generator using C++ coroutines:

```cpp
auto range(int start, int end) -> generator<int> {
  for (int i = start; i < end; ++i)
    co_yield i;
}

for (int x : range(0, 10)) { ... }
```

## SI Literals — `tenzir/si_literals.hpp`

```cpp
using namespace tenzir::si_literals;
auto thousand = 1_k;     // 1,000
auto million = 1_M;      // 1,000,000
auto billion = 1_G;      // 1,000,000,000

auto kibibyte = 1_Ki;    // 1,024
auto mebibyte = 1_Mi;    // 1,048,576
auto gibibyte = 1_Gi;    // 1,073,741,824

// Byte variants
using namespace tenzir::binary_byte_literals;
auto size = 64_MiB;      // 67,108,864

using namespace tenzir::decimal_byte_literals;
auto size = 100_MB;      // 100,000,000
```

## Glob — `tenzir/glob.hpp`

```cpp
auto pattern = parse_glob("*.txt");
if (matches("file.txt", pattern)) { ... }

// Supports: * (any), ** (recursive), literal strings
auto recursive = parse_glob("src/**/*.cpp");
```

## Chunk — `tenzir/chunk.hpp`

Reference-counted memory buffer:

```cpp
auto chunk = chunk::make(bytes);
auto chunk = chunk::mmap(path);         // Memory-mapped file

auto view = chunk->view();              // std::span<const std::byte>
auto slice = chunk->slice(offset, len); // Slice without copying
```

## Concepts — `tenzir/concepts.hpp`

Common C++ concepts for generic programming:

- `concepts::container` — Types with `std::data`/`std::size`
- `concepts::byte_container` — Contiguous byte buffers
- `concepts::byte_sequence` — Types convertible to `std::span<const std::byte>`
- `concepts::number` — Integers and floats
- `concepts::transparent` — Types with `is_transparent` (heterogeneous lookup)
