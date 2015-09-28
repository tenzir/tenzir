#include "xxhash/xxhash.h"

#include "vast/util/assert.h"
#include "vast/util/hash/xxhash.h"

namespace vast {
namespace util {

xxhash32::xxhash32(digest_type seed) {
  static_assert(sizeof(xxhash32::state_type) == sizeof(XXH32_state_t)
                && alignof(xxhash32::state_type) == alignof(XXH32_state_t),
                "xxhash32 state types out of sync");
  auto state = reinterpret_cast<XXH32_state_t*>(&state_);
  ::XXH32_reset(state, seed);
}

xxhash32::digest_type
xxhash32::value(void const* x, size_t n, digest_type seed) {
  VAST_ASSERT(n <= (1u << 31) - 1);
  return ::XXH32(x, n, seed);
}

bool xxhash32::update(void const* x, size_t n) {
  VAST_ASSERT(n <= (1u << 31) - 1);
  auto state = reinterpret_cast<XXH32_state_t*>(&state_);
  return ::XXH32_update(state, x, n) == XXH_OK;
}

xxhash32::digest_type xxhash32::compute() {
  auto state = reinterpret_cast<XXH32_state_t*>(&state_);
  return ::XXH32_digest(state);
}


xxhash64::xxhash64(digest_type seed) {
  static_assert(sizeof(xxhash64::state_type) == sizeof(XXH64_state_t)
                && alignof(xxhash64::state_type) == alignof(XXH64_state_t),
                "xxhash64 state types out of sync");
  auto state = reinterpret_cast<XXH64_state_t*>(&state_);
  ::XXH64_reset(state, seed);
}

xxhash64::digest_type
xxhash64::value(void const* x, size_t n, digest_type seed) {
  return ::XXH64(x, n, seed);
}

bool xxhash64::update(void const* x, size_t n) {
  auto state = reinterpret_cast<XXH64_state_t*>(&state_);
  return ::XXH64_update(state, x, n) == XXH_OK;
}

xxhash64::digest_type xxhash64::compute() {
  auto state = reinterpret_cast<XXH64_state_t*>(&state_);
  return ::XXH64_digest(state);
}

} // namespace util
} // namespace vast
