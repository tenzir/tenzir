/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#define XXH_ACCEPT_NULL_INPUT_POINTER
#define XXH_PRIVATE_API
#define XXH_STATIC_LINKING_ONLY
#include "xxhash/xxhash.h"

#include "vast/concept/hashable/xxhash.hpp"
#include "vast/detail/assert.hpp"

namespace vast {

xxhash32::xxhash32(result_type seed) noexcept {
  static_assert(sizeof(xxhash32::state_type) == sizeof(XXH32_state_t)
                && alignof(xxhash32::state_type) == alignof(XXH32_state_t),
                "xxhash32 state types out of sync");
  auto state = reinterpret_cast<XXH32_state_t*>(&state_);
  ::XXH32_reset(state, seed);
}

void xxhash32::operator()(void const* x, size_t n) noexcept {
  VAST_ASSERT(n <= (1u << 31) - 1);
  auto state = reinterpret_cast<XXH32_state_t*>(&state_);
  auto result = ::XXH32_update(state, x, n);
  VAST_ASSERT(result == XXH_OK);
}

xxhash32::operator result_type() noexcept {
  auto state = reinterpret_cast<XXH32_state_t*>(&state_);
  return ::XXH32_digest(state);
}


xxhash64::xxhash64(result_type seed) noexcept {
  static_assert(sizeof(xxhash64::state_type) == sizeof(XXH64_state_t)
                && alignof(xxhash64::state_type) == alignof(XXH64_state_t),
                "xxhash64 state types out of sync");
  auto state = reinterpret_cast<XXH64_state_t*>(&state_);
  ::XXH64_reset(state, seed);
}

void xxhash64::operator()(void const* x, size_t n) noexcept {
  auto state = reinterpret_cast<XXH64_state_t*>(&state_);
  auto result = ::XXH64_update(state, x, n);
  VAST_ASSERT(result == XXH_OK);
}

xxhash64::operator result_type() noexcept {
  auto state = reinterpret_cast<XXH64_state_t*>(&state_);
  return ::XXH64_digest(state);
}

} // namespace vast
