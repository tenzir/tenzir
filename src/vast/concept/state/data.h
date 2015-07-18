#ifndef VAST_CONCEPT_STATE_DATA_H
#define VAST_CONCEPT_STATE_DATA_H

#include "vast/data.h"
#include "vast/concept/state/address.h"
#include "vast/concept/state/pattern.h"
#include "vast/concept/state/port.h"
#include "vast/concept/state/subnet.h"
#include "vast/concept/state/time.h"
#include "vast/concept/state/none.h"
#include "vast/util/meta.h"

namespace vast {

template <>
struct access::state<vector>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    using base = util::deduce<decltype(x), std::vector<data>>;
    f(static_cast<base>(x));
  }
};

template <>
struct access::state<set>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    using base = util::deduce<decltype(x), util::flat_set<data>>;
    f(static_cast<base>(x));
  }
};

template <>
struct access::state<table>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    using base = util::deduce<decltype(x), std::map<data, data>>;
    f(static_cast<base>(x));
  }
};

template <>
struct access::state<record>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    using base = util::deduce<decltype(x), std::vector<data>>;
    f(static_cast<base>(x));
  }
};

template <>
struct access::state<data>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    f(x.data_);
  }
};

} // namespace vast

#endif
