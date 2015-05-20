#ifndef VAST_UTIL_HASH_XXHASH_H
#define VAST_UTIL_HASH_XXHASH_H

#include "vast/util/assert.h"
#include "vast/util/hash.h"

namespace vast {
namespace util {
namespace detail {

enum XXH_errorcode { XXH_OK=0, XXH_ERROR };

unsigned int XXH32 (void const* input, int len, unsigned seed);

void* XXH32_init (unsigned seed);
XXH_errorcode XXH32_update (void* state, void const* input, int len);
unsigned XXH32_digest (void* state);

int XXH32_sizeofState(void);
XXH_errorcode XXH32_resetState(void* state, unsigned seed);

#define XXH32_SIZEOFSTATE 48
struct XXH32_stateSpace_t
{
  long long ll[(XXH32_SIZEOFSTATE + (sizeof(long long)-1)) / sizeof(long long)];
};

unsigned int XXH32_intermediateDigest(void* state);

} // namespace detail

/// The [xxhash](https://code.google.com/p/xxhash/) algorithm.
class xxhash : public hash<xxhash>
{
  friend hash<xxhash>;

public:
  using digest_type = unsigned;
  using state_type = detail::XXH32_stateSpace_t;

  /// The maximum length of an xxhash computation.
  static constexpr size_t max_len = (1u << 31) - 1;

  explicit xxhash(uint32_t seed = 0)
  {
    detail::XXH32_resetState(&state_, seed);
  }

  explicit xxhash(state_type state)
    : state_(state)
  {
  }

  state_type const& state() const
  {
    return state_;
  }

private:
  /// @pre `n <= max`
  static digest_type value(void const* x, size_t n, uint32_t seed = 0)
  {
    VAST_ASSERT(n <= max_len);
    return detail::XXH32(x, static_cast<int>(n), seed);
  }

  /// @pre `n <= max`
  bool update(void const* x, size_t n)
  {
    VAST_ASSERT(n <= max_len);
    return detail::XXH32_update(&state_, x, static_cast<size_t>(n))
        == detail::XXH_OK;
  }

  digest_type compute()
  {
    return detail::XXH32_intermediateDigest(&state_);
  }

  state_type state_;
};

} // namespace util
} // namespace vast

#endif
