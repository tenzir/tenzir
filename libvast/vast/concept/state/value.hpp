#ifndef VAST_CONCEPT_STATE_VALUE_HPP
#define VAST_CONCEPT_STATE_VALUE_HPP

#include "vast/value.hpp"
#include "vast/concept/state/data.hpp"
#include "vast/concept/state/type.hpp"

namespace vast {

template <>
struct access::state<value> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.data_, x.type_);
  }
};

} // namespace vast

#endif
