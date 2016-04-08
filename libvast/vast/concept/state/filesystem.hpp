#ifndef VAST_CONCEPT_STATE_FILESYSTEM_HPP
#define VAST_CONCEPT_STATE_FILESYSTEM_HPP

#include "vast/access.hpp"

namespace vast {

class path;

template <>
struct access::state<path> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.str_);
  }
};

} // namespace vast

#endif
