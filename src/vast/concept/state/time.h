#ifndef VAST_CONCEPT_STATE_TIME_H
#define VAST_CONCEPT_STATE_TIME_H

#include "vast/access.h"

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
