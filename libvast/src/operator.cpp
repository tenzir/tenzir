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

#include "vast/die.hpp"
#include "vast/operator.hpp"

namespace vast {

bool is_negated(relational_operator op) {
  switch (op) {
    case relational_operator::equal:
    case relational_operator::match:
    case relational_operator::less:
    case relational_operator::less_equal:
    case relational_operator::greater:
    case relational_operator::greater_equal:
    case relational_operator::in:
    case relational_operator::ni:
      return false;
    case relational_operator::not_ni:
    case relational_operator::not_in:
    case relational_operator::not_equal:
    case relational_operator::not_match:
      return true;
  }
  die("missing case for relational operator");
}

relational_operator negate(relational_operator op) {
  switch (op) {
    case relational_operator::match:
      return relational_operator::not_match;
    case relational_operator::not_match:
      return relational_operator::match;
    case relational_operator::equal:
      return relational_operator::not_equal;
    case relational_operator::not_equal:
      return relational_operator::equal;
    case relational_operator::less:
      return relational_operator::greater_equal;
    case relational_operator::less_equal:
      return relational_operator::greater;
    case relational_operator::greater:
      return relational_operator::less_equal;
    case relational_operator::greater_equal:
      return relational_operator::less;
    case relational_operator::in:
      return relational_operator::not_in;
    case relational_operator::not_in:
      return relational_operator::in;
    case relational_operator::ni:
      return relational_operator::not_ni;
    case relational_operator::not_ni:
      return relational_operator::ni;
  }
  die("missing case for relational operator");
}

relational_operator flip(relational_operator op) {
  switch (op) {
    case relational_operator::match:
    case relational_operator::not_match:
    case relational_operator::equal:
    case relational_operator::not_equal:
      return op;
    case relational_operator::less:
      return relational_operator::greater;
    case relational_operator::less_equal:
      return relational_operator::greater_equal;
    case relational_operator::greater:
      return relational_operator::less;
    case relational_operator::greater_equal:
      return relational_operator::less_equal;
    case relational_operator::in:
      return relational_operator::ni;
    case relational_operator::not_in:
      return relational_operator::not_ni;
    case relational_operator::ni:
      return relational_operator::in;
    case relational_operator::not_ni:
      return relational_operator::not_in;
  }
  die("missing case for relational operator");
}

} // namespace vast
