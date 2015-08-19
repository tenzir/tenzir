#ifndef VAST_CONCEPT_STATE_VALUE_H
#define VAST_CONCEPT_STATE_VALUE_H

#include "vast/value.h"
#include "vast/concept/state/data.h"
#include "vast/concept/state/type.h"

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
