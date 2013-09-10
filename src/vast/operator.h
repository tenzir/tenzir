#ifndef VAST_OPERATOR_H
#define VAST_OPERATOR_H

#include <cstdint>
#include <string>

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

bool convert(arithmetic_operator op, std::string& to);

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

bool convert(relational_operator op, std::string& to);

/// A boolean operator.
enum boolean_operator : uint8_t
{
  logical_not,
  logical_and,
  logical_or
};

bool convert(boolean_operator op, std::string& to);

/// Negates a relational operator, i.e., creates the complementary operator.
/// @param op The operator to negate.
/// @return The complement of *op*.
relational_operator negate(relational_operator op);

} // namespace

#endif
