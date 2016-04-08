#ifndef VAST_CONCEPT_STATE_SUBNET_HPP
#define VAST_CONCEPT_STATE_SUBNET_HPP

#include "vast/concept/state/address.hpp"

namespace vast {

class subnet;

template <>
struct access::state<subnet> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.network_, x.length_);
  }
};

} // namespace vast

#endif
