#ifndef VAST_CONCEPT_STATE_UTIL_ERROR_H
#define VAST_CONCEPT_STATE_UTIL_ERROR_H

#include "vast/util/error.h"

namespace vast {

template <>
struct access::state<util::error>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    f(x.msg_);
  }
};

} // namespace vast

#endif
