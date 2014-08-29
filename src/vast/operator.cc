#include "vast/operator.h"
#include "vast/logger.h"
#include "vast/serialization/arithmetic.h"

namespace vast {

void serialize(serializer& sink, arithmetic_operator op)
{
  sink << static_cast<std::underlying_type<arithmetic_operator>::type>(op);
}

void deserialize(deserializer& source, arithmetic_operator& op)
{
  std::underlying_type<arithmetic_operator>::type u;
  source >> u;
  op = static_cast<arithmetic_operator>(u);
}


void serialize(serializer& sink, relational_operator op)
{
  sink << static_cast<std::underlying_type<relational_operator>::type>(op);
}

void deserialize(deserializer& source, relational_operator& op)
{
  std::underlying_type<relational_operator>::type u;
  source >> u;
  op = static_cast<relational_operator>(u);
}


void serialize(serializer& sink, boolean_operator op)
{
  sink << static_cast<std::underlying_type<boolean_operator>::type>(op);
}

void deserialize(deserializer& source, boolean_operator& op)
{
  std::underlying_type<boolean_operator>::type u;
  source >> u;
  op = static_cast<boolean_operator>(u);
}

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
    case ni:
      return not_ni;
    case not_ni:
      return ni;
  }
}

relational_operator flip(relational_operator op)
{
  switch (op)
  {
    default:
      throw std::logic_error("missing case for relational operator");
    case match:
    case not_match:
    case equal:
    case not_equal:
      return op;
    case less:
      return greater_equal;
    case less_equal:
      return greater;
    case greater:
      return less_equal;
    case greater_equal:
      return less;
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
