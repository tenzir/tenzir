#ifndef VAST_OPERATOR_H
#define VAST_OPERATOR_H

namespace vast {

/// An arithmetic operator.
enum arithmetic_operator : uint8_t
{
  // Unary
  positive,
  negative,
  bitwise_not,
  // Binary
  bitwise_or,
  bitwise_xor,
  bitwise_and,
  plus,
  minus,
  times,
  divides,
  mod
};

/// A (binary) relational operator.
enum relational_operator : uint8_t
{
  match,
  not_match,
  in,
  not_in,
  equal,
  not_equal,
  less,
  less_equal,
  greater,
  greater_equal
};

/// A boolean operator.
enum boolean_operator : uint8_t
{
  logical_not,
  logical_and,
  logical_or
};

/// Negates a relational operator, i.e., creates the complementary operator.
/// @param op The operator to negate.
/// @return The complement of *op*.
inline relational_operator negate(relational_operator op)
{
  switch (op)
  {
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

} // namespace

#endif
