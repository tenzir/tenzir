//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <cstdint>
#include <string>

namespace vast {

/// An arithmetic operator.
enum class arithmetic_operator : uint8_t {
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

/// @relates arithmetic_operator
std::string to_string(arithmetic_operator op) noexcept;

/// A (binary) relational operator.
enum class relational_operator : uint8_t {
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

/// @relates relational_operator
std::string to_string(relational_operator op) noexcept;

/// A boolean operator taking on the values AND, OR, and NOT.
enum class bool_operator : uint8_t {
  logical_not,
  logical_and,
  logical_or,
};

/// @relates bool_operator
std::string to_string(bool_operator op) noexcept;

/// Tests wheter a relational operator is is_negated.
/// For example, `!=` is is_negated, but `==` is not.
/// @param op The operator to negate.
/// @returns `true` if the operator is is_negated.
bool is_negated(relational_operator op);

/// Negates a relational operator by creating the complent.
/// For example, `==` becomes `!=`.
/// @param op The operator to negate.
/// @returns The complement of *op*.
relational_operator negate(relational_operator op);

/// Flips the directionality of an asymmetric operator.
/// I.e., for a given predicate *P = LHS op RHS*, the function returns the
/// operator such that the predicate *RHS op LHS* is equivalent to *P*.
/// @param op The operator to flip.
/// @returns The flipped operator.
relational_operator flip(relational_operator op);

} // namespace vast
