/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#ifndef VAST_OPERATOR_HPP
#define VAST_OPERATOR_HPP

#include <cstdint>
#include <string>

namespace vast {

/// An arithmetic operator.
enum arithmetic_operator : uint8_t {
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
enum relational_operator : uint8_t {
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

/// A boolean operator taking on the values AND, OR, and NOT.
enum boolean_operator : uint8_t {
  logical_not,
  logical_and,
  logical_or
};

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

#endif
