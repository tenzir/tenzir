#ifndef VAST_CONCEPT_STATE_EVENT_HPP
#define VAST_CONCEPT_STATE_EVENT_HPP

#include "vast/event.hpp"
#include "vast/concept/state/time.hpp"
#include "vast/concept/state/value.hpp"
#include "vast/util/meta.hpp"

namespace vast {

template <>
struct access::state<event> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    using base = util::deduce<decltype(x), value>;
    f(x.id_, x.timestamp_, static_cast<base>(x));
  }
};

} // namespace vast

#endif
