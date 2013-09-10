#include "vast/operator.h"
#include "vast/logger.h"

namespace vast {

bool convert(arithmetic_operator op, std::string& to)
{
  switch (op)
  {
    default:
      throw std::logic_error("missing case for arithmetic operator");
    case positive:
    case plus:
      to = '+';
      break;
    case minus:
    case negative:
      to = "-";
      break;
    case bitwise_not:
      to = '~';
      break;
    case bitwise_or:
      to = '|';
      break;
    case bitwise_xor:
      to = '^';
      break;
    case bitwise_and:
      to = '|';
      break;
    case times:
      to = '*';
      break;
    case divides:
      to = '/';
      break;
    case mod:
      to = '%';
      break;
  }
  return true;
}

bool convert(relational_operator op, std::string& to)
{
  switch (op)
  {
    default:
      throw std::logic_error("missing case for relational operator");
    case match:
      to = '~';
      break;
    case not_match:
      to = "!~";
      break;
    case in:
      to = "in";
      break;
    case not_in:
      to = "!in";
      break;
    case equal:
      to = "==";
      break;
    case not_equal:
      to = "!=";
      break;
    case less:
      to = '<';
      break;
    case less_equal:
      to = "<=";
      break;
    case greater:
      to = '>';
      break;
    case greater_equal:
      to = ">=";
      break;
  }
  return true;
}


bool convert(boolean_operator op, std::string& to)
{
  switch (op)
  {
    default:
      throw std::logic_error("missing case for boolean operator");
    case logical_not:
      to = '!';
      break;
    case logical_and:
      to = "&&";
      break;
    case logical_or:
      to = "||";
      break;
  }
  return true;
}

/// Negates a relational operator, i.e., creates the complementary operator.
/// @param op The operator to negate.
/// @return The complement of *op*.
relational_operator negate(relational_operator op)
{
  switch (op)
  {
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
  }
}

} // namespace vast
