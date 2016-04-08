#include <stdexcept>

#include "vast/operator.hpp"

namespace vast {

relational_operator negate(relational_operator op) {
  switch (op) {
    default:
      throw std::logic_error("missing case for relational operator");
    case match:
      return not_match;
    case not_match:
      return match;
    case equal:
      return not_equal;
    case not_equal:
      return equal;
    case less:
      return greater_equal;
    case less_equal:
      return greater;
    case greater:
      return less_equal;
    case greater_equal:
      return less;
    case in:
      return not_in;
    case not_in:
      return in;
    case ni:
      return not_ni;
    case not_ni:
      return ni;
  }
}

relational_operator flip(relational_operator op) {
  switch (op) {
    default:
      throw std::logic_error("missing case for relational operator");
    case match:
    case not_match:
    case equal:
    case not_equal:
      return op;
    case less:
      return greater;
    case less_equal:
      return greater_equal;
    case greater:
      return less;
    case greater_equal:
      return less_equal;
    case in:
      return ni;
    case not_in:
      return not_ni;
    case ni:
      return in;
    case not_ni:
      return not_in;
  }
}

} // namespace vast
