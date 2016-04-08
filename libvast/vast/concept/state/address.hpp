#ifndef VAST_CONCEPT_STATE_ADDRESS_HPP
#define VAST_CONCEPT_STATE_ADDRESS_HPP

#include "vast/access.hpp"

namespace vast {

class address;

template <>
struct access::state<address> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.bytes_);
  }
};

} // namespace vast

#endif
