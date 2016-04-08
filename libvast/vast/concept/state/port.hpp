#ifndef VAST_CONCEPT_STATE_PORT_HPP
#define VAST_CONCEPT_STATE_PORT_HPP

#include "vast/access.hpp"

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
