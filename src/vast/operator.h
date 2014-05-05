#ifndef VAST_OPERATOR_H
#define VAST_OPERATOR_H

#include <cstdint>
#include <string>
#include "vast/fwd.h"
#include "vast/print.h"

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

void serialize(serializer& sink, arithmetic_operator op);
void deserialize(deserializer& source, arithmetic_operator& op);

template <typename Iterator>
trial<void> print(arithmetic_operator op, Iterator&& out)
{
  switch (op)
  {
    default:
      throw std::logic_error("missing case for arithmetic operator");
    case positive:
    case plus:
      return print('+', out);
    case minus:
    case negative:
      return print('-', out);
    case bitwise_not:
      return print('~', out);
    case bitwise_or:
      return print('|', out);
    case bitwise_xor:
      return print('^', out);
    case bitwise_and:
      return print('|', out);
    case times:
      return print('*', out);
    case divides:
      return print('/', out);
    case mod:
      return print('%', out);
  }

  return nothing;
}

/// A (binary) relational operator.
enum relational_operator : uint8_t
{
  match,
  not_match,
  in,
  not_in,
  ni,
  not_ni,
  equal,
  not_equal,
  less,
  less_equal,
  greater,
  greater_equal
};

void serialize(serializer& sink, relational_operator op);
void deserialize(deserializer& source, relational_operator& op);

template <typename Iterator>
trial<void> print(relational_operator op, Iterator&& out)
{
  switch (op)
  {
    default:
      throw std::logic_error("missing case for relational operator");
    case match:
      return print('~', out);
    case not_match:
      return print("!~", out);
    case in:
      return print("in", out);
    case not_in:
      return print("!in", out);
    case ni:
      return print("ni", out);
    case not_ni:
      return print("!ni", out);
    case equal:
      return print("==", out);
    case not_equal:
      return print("!=", out);
    case less:
      return print('<', out);
    case less_equal:
      return print("<=", out);
    case greater:
      return print('>', out);
    case greater_equal:
      return print(">=", out);
  }

  return nothing;
}


/// A boolean operator.
enum boolean_operator : uint8_t
{
  logical_not,
  logical_and,
  logical_or
};

void serialize(serializer& sink, boolean_operator op);
void deserialize(deserializer& source, boolean_operator& op);

template <typename Iterator>
trial<void> print(boolean_operator op, Iterator&& out)
{
  switch (op)
  {
    default:
      throw std::logic_error("missing case for boolean operator");
    case logical_not:
      return print("!", out);
    case logical_and:
      return print("&&", out);
    case logical_or:
      return print("||", out);
  }

  return nothing;
}

/// Negates a relational operator, i.e., creates the complementary operator.
/// @param op The operator to negate.
/// @returns The complement of *op*.
relational_operator negate(relational_operator op);

} // namespace

#endif
