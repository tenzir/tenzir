#ifndef VAST_CONCEPT_HASHABLE_XXHASH_HPP
#define VAST_CONCEPT_HASHABLE_XXHASH_HPP

#include <cstddef>
#include <type_traits>

#include "vast/detail/endian.hpp"

namespace vast {

struct xxhash_base {
  using result_type = size_t;

  // If XXH_FORCE_NATIVE_FORMAT == 1 in xxhash.c, then use host_endian.
  static constexpr detail::endianness endian = detail::little_endian;
};

/// The 32-bit version of xxHash.
class xxhash32 : public xxhash_base {
public:
  explicit xxhash32(result_type seed = 0) noexcept;

  void operator()(void const* x, size_t n) noexcept;

  explicit operator result_type() noexcept;

  template <class Inspector>
  friend auto inspect(Inspector& f, xxhash32& xxh) {
    return f(xxh.state_);
  }

private:
  struct state_type { long long ll[ 6]; };

  state_type state_;
};

/// The 64-bit version of xxHash.
class xxhash64 : public xxhash_base {
public:
  explicit xxhash64(result_type seed = 0) noexcept;

  void operator()(void const* x, size_t n) noexcept;

  explicit operator result_type() noexcept;

  template <class Inspector>
  friend auto inspect(Inspector& f, xxhash64& xxh) {
    return f(xxh.state_);
  }

private:
  struct state_type { long long ll[11]; };

  state_type state_;
};

/// The [xxhash](https://github.com/Cyan4973/xxHash) algorithm.
using xxhash = std::conditional_t<sizeof(void*) == 4, xxhash32, xxhash64>;

} // namespace vast

#endif
