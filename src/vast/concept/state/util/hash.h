#ifndef VAST_CONCEPT_STATE_UTIL_HASH_H
#define VAST_CONCEPT_STATE_UTIL_HASH_H

#include "vast/access.h"
#include "vast/util/hash/xxhash.h"

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
