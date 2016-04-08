#ifndef VAST_CONCEPT_STATE_UTIL_HASH_HPP
#define VAST_CONCEPT_STATE_UTIL_HASH_HPP

#include "vast/access.hpp"
#include "vast/util/hash/xxhash.hpp"

namespace vast {

template <>
struct access::state<util::xxhash32> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.state_.ll);
  }
};

template <>
struct access::state<util::xxhash64> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.state_.ll);
  }
};

} // namespace vast

#endif
