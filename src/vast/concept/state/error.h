#ifndef VAST_CONCEPT_STATE_ERROR_H
#define VAST_CONCEPT_STATE_ERROR_H

#include "vast/error.h"

namespace vast {

template <>
struct access::state<error>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    f(x.msg_);
  }
};

} // namespace vast

#endif
