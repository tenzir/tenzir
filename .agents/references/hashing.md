# Hashing

Generic hashing infrastructure in `tenzir/hash/`.

## Usage

```cpp
#include <tenzir/hash/hash.hpp>

auto digest = hash(value);                    // Default algorithm (xxh3_64)
auto digest = hash(value1, value2, value3);   // Multiple values
auto digest = hash<xxh64>(value);             // Specific algorithm
auto digest = seeded_hash{seed}(value);       // Seeded
```

## Algorithms

| Algorithm  | Header            | Description                    |
| ---------- | ----------------- | ------------------------------ |
| `xxh3_64`  | `hash/xxhash.hpp` | Default, fast 64-bit (xxHash3) |
| `xxh3_128` | `hash/xxhash.hpp` | 128-bit variant                |
| `xxh64`    | `hash/xxhash.hpp` | Classic xxHash 64-bit          |
| `fnv<64>`  | `hash/fnv.hpp`    | FNV-1a 64-bit                  |
| `sha1`     | `hash/sha.hpp`    | SHA-1 (OpenSSL)                |
| `sha256`   | `hash/sha.hpp`    | SHA-256 (OpenSSL)              |
| `sha512`   | `hash/sha.hpp`    | SHA-512 (OpenSSL)              |
| `md5`      | `hash/md5.hpp`    | MD5 (OpenSSL)                  |
| `crc32`    | `hash/crc.hpp`    | CRC-32                         |

## Extending to Custom Types

Types with `inspect()` are automatically hashable. Otherwise, provide a
`hash_append` overload:

```cpp
template <class HashAlgorithm>
void hash_append(HashAlgorithm& h, const my_type& x) noexcept {
  hash_append(h, x.name);
  hash_append(h, x.value);
}
```

Built-in types automatically supported: scalars, strings, vectors, arrays,
pairs, tuples, optionals, variants, sets, maps.

## Key Headers

- `tenzir/hash/hash.hpp` — `hash()` and `seeded_hash`
- `tenzir/hash/hash_append.hpp` — Extension point for custom types
- `tenzir/hash/concepts.hpp` — `oneshot_hash`, `incremental_hash` concepts
