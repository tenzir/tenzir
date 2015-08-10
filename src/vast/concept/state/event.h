#ifndef VAST_CONCEPT_STATE_EVENT_H
#define VAST_CONCEPT_STATE_EVENT_H

#include "vast/event.h"
#include "vast/concept/state/time.h"
#include "vast/concept/state/value.h"
#include "vast/util/meta.h"

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
