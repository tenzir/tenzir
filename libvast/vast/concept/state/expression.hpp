#ifndef VAST_CONCEPT_STATE_EXPRESSION_HPP
#define VAST_CONCEPT_STATE_EXPRESSION_HPP

#include "vast/access.hpp"
#include "vast/expression.hpp"

namespace vast {

template <>
struct access::state<expression> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.node_);
  }
};

} // namespace vast

#endif
