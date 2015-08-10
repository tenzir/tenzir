#ifndef VAST_CONCEPT_STATE_PORT_H
#define VAST_CONCEPT_STATE_PORT_H

#include "vast/access.h"

namespace vast {

class port;

template <>
struct access::state<port> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.number_, x.type_);
  }
};

} // namespace vast

#endif
