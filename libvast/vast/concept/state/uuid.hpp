#ifndef VAST_CONCEPT_STATE_UUID_HPP
#define VAST_CONCEPT_STATE_UUID_HPP

#include "vast/access.hpp"

namespace vast {

class uuid;

template <>
struct access::state<uuid> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.id_);
  }
};

} // namespace vast

#endif
