#ifndef VAST_CONCEPT_STATE_TIME_HPP
#define VAST_CONCEPT_STATE_TIME_HPP

#include "vast/access.hpp"

namespace vast {

namespace time {
class duration;
class point;
}

template <>
struct access::state<time::duration> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.duration_);
  }
};

template <>
struct access::state<time::point> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.time_point_);
  }
};

} // namespace vast

#endif
