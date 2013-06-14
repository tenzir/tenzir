#include "vast/operator.h"

#include "vast/exception.h"

namespace vast {

/// Negates a relational operator, i.e., creates the complementary operator.
/// @param op The operator to negate.
/// @return The complement of *op*.
relational_operator negate(relational_operator op)
{
  switch (op)
  {
    default:
      throw error::logic("missing case for relational operator");
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
  }
}

} // namespace vast
