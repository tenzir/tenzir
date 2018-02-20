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

relational_operator negate(relational_operator op) {
  switch (op) {
    default:
      die("missing case for relational operator");
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
      die("missing case for relational operator");
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
