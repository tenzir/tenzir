#ifndef VAST_CONCEPT_STATE_JSON_H
#define VAST_CONCEPT_STATE_JSON_H

#include "vast/json.h"

namespace vast {

template <>
struct access::state<json> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.value_);
  }
};

} // namespace vast

#endif
