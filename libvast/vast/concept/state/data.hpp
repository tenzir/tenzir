#ifndef VAST_CONCEPT_STATE_DATA_HPP
#define VAST_CONCEPT_STATE_DATA_HPP

#include "vast/data.hpp"
#include "vast/concept/state/address.hpp"
#include "vast/concept/state/pattern.hpp"
#include "vast/concept/state/port.hpp"
#include "vast/concept/state/subnet.hpp"
#include "vast/concept/state/time.hpp"
#include "vast/util/meta.hpp"

namespace vast {

template <>
struct access::state<vector> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    using base = util::deduce<decltype(x), std::vector<data>>;
    f(static_cast<base>(x));
  }
};

template <>
struct access::state<set> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    using base = util::deduce<decltype(x), util::flat_set<data>>;
    f(static_cast<base>(x));
  }
};

template <>
struct access::state<table> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    using base = util::deduce<decltype(x), std::map<data, data>>;
    f(static_cast<base>(x));
  }
};

template <>
struct access::state<record> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    using base = util::deduce<decltype(x), std::vector<data>>;
    f(static_cast<base>(x));
  }
};

template <>
struct access::state<data> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.data_);
  }
};

} // namespace vast

#endif
