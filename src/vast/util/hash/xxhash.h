#ifndef VAST_UTIL_HASH_XXHASH_H
#define VAST_UTIL_HASH_XXHASH_H

#include <type_traits>

#include "vast/util/hash.h"

namespace vast {

struct access;

namespace util {

/// The 32-bit version of xxHash.
class xxhash32 : public hash<xxhash32> {
  friend access;
  friend hash<xxhash32>;

public:
  using digest_type = unsigned;

  explicit xxhash32(digest_type seed = 0);

private:
  static digest_type value(void const* x, size_t n, digest_type seed = 0);

  bool update(void const* x, size_t n);

  digest_type compute();

  struct state_type { long long ll[ 6]; };

  state_type state_;
};

/// The 64-bit version of xxHash.
class xxhash64 : public hash<xxhash64> {
  friend access;
  friend hash<xxhash64>;

public:
  using digest_type = unsigned long long;

  explicit xxhash64(digest_type seed = 0);

private:
  static digest_type value(void const* x, size_t n, digest_type seed = 0);

  bool update(void const* x, size_t n);

  digest_type compute();

  struct state_type { long long ll[11]; };

  state_type state_;
};

/// The [xxhash](https://github.com/Cyan4973/xxHash) algorithm.
using xxhash = std::conditional_t<sizeof(void*) == 4, xxhash32, xxhash64>;

} // namespace util
} // namespace vast

#endif
