#ifndef VAST_CONCEPT_STATE_EXPRESSION_H
#define VAST_CONCEPT_STATE_EXPRESSION_H

#include "vast/access.h"
#include "vast/expression.h"

namespace vast {

template <>
struct access::state<expression>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    f(x.node_);
  }
};

} // namespace vast

#endif
