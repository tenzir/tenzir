#ifndef VAST_CONCEPT_STATE_NONE_H
#define VAST_CONCEPT_STATE_NONE_H

#include "vast/none.h"

namespace vast {

template <>
struct access::state<none>
{
  template <typename T, typename F>
  static void call(T&&, F)
  {
    // nop
  }
};

} // namespace vast

#endif
