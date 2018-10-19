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

#pragma once

#include <caf/intrusive_ptr.hpp>
#include <caf/ref_counted.hpp>

#include "vast/operator.hpp"
#include "vast/synopsis.hpp"
#include "vast/view.hpp"

#include "vast/detail/assert.hpp"

namespace vast {

/// The abstract base class for synopsis data structures.
class synopsis : public caf::ref_counted {
public:
  virtual ~synopsis();

  /// Adds data from a table slice.
  /// @param slice The table slice to process.
  virtual void add(data_view x) = 0;

  /// Tests whether a predicate matches. The synopsis is implicitly the LHS of
  /// the predicate.
  /// @param op The operator of the predicate.
  /// @param rhs The RHS of the predicate.
  /// @returns The evaluation result of `*this op rhs`.
  virtual bool lookup(relational_operator op, data_view rhs) const = 0;
};

/// @relates synopsis
using synopsis_ptr = caf::intrusive_ptr<synopsis>;

/// A synopsis structure that keeps track of the minimum and maximum value.
template <class T>
class min_max_synopsis : public synopsis {
public:
  min_max_synopsis(T min = T{}, T max = T{}) : min_{min}, max_{max} {
    // nop
  }

  void add(data_view x) override {
    auto y = caf::get_if<view<T>>(&x);
    VAST_ASSERT(y != nullptr);
    if (*y < min_)
      min_ = *y;
    if (*y > max_)
      max_ = *y;
  }

  bool lookup(relational_operator op, data_view rhs) const override {
    // There are 5 possible scenarios to differentiate for the inputs:
    //
    //   (1) rhs < min
    //   (2) rhs == min
    //   (3) rhs >= min && <= max
    //   (4) rhs == max
    //   (5) rhs > max
    //
    // For each possibility, we need to make sure that the expression `[min,
    // max] op rhs` remains valid. Here is an example for operator <:
    //
    //   (1) [4,8] < 1 is false (4 < 1 || 8 < 1)
    //   (2) [4,8] < 4 is false (4 < 4 || 8 < 4)
    //   (3) [4,8] < 5 is true  (4 < 5 || 8 < 5)
    //   (4) [4,8] < 8 is true  (4 < 8 || 8 < 8)
    //   (5) [4,8] < 9 is true  (4 < 9 || 8 < 9)
    //
    // Thus, for range comparisons we need to test `min op rhs || max op rhs`.
    auto x = caf::get_if<view<T>>(&rhs);
    VAST_ASSERT(x != nullptr);
    switch (op) {
      default:
        VAST_ASSERT(!"unsupported operator");
        return false;
      case equal:
        return min_ <= *x && *x <= max_;
      case not_equal:
        return !(min_ <= *x && *x <= max_);
      case less:
        return min_ < *x || max_ < *x;
      case less_equal:
        return min_ <= *x || max_ <= *x;
      case greater:
        return min_ > *x || max_ > *x;
      case greater_equal:
        return min_ >= *x || max_ >= *x;
    }
  }

private:
  T min_;
  T max_;
};

} // namespace vast

