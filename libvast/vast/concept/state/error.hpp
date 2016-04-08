#ifndef VAST_CONCEPT_STATE_ERROR_HPP
#define VAST_CONCEPT_STATE_ERROR_HPP

#include "vast/error.hpp"

namespace vast {

template <>
struct access::state<error> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.msg_);
  }
};

} // namespace vast

#endif
