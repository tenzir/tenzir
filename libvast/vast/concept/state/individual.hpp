#ifndef VAST_CONCEPT_STATE_INDIVIDUAL_HPP
#define VAST_CONCEPT_STATE_INDIVIDUAL_HPP

#include "vast/access.hpp"

namespace vast {

class individual;

template <>
struct access::state<individual> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.id_);
  }
};

} // namespace vast

#endif
