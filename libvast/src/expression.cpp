#include "vast/expression.hpp"
#include "vast/util/assert.hpp"

namespace vast {

expression const& negation::expression() const {
  VAST_ASSERT(!empty());
  return *begin();
}

expression& negation::expression() {
  VAST_ASSERT(!empty());
  return *begin();
}

expression::node& expose(expression& e) {
  return e.node_;
}

expression::node const& expose(expression const& e) {
  return e.node_;
}

bool operator==(expression const& lhs, expression const& rhs) {
  return lhs.node_ == rhs.node_;
}

bool operator<(expression const& lhs, expression const& rhs) {
  return lhs.node_ < rhs.node_;
}

} // namespace vast
