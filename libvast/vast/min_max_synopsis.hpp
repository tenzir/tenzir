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

#include <caf/deserializer.hpp>
#include <caf/optional.hpp>
#include <caf/serializer.hpp>
#include <caf/sum_type.hpp>

#include "vast/synopsis.hpp"

namespace vast {

/// A synopsis structure that keeps track of the minimum and maximum value.
template <class T>
class min_max_synopsis : public synopsis {
public:
  min_max_synopsis(vast::type x, T min = T{}, T max = T{})
    : synopsis{std::move(x)},
      min_{min},
      max_{max} {
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

  caf::optional<bool> lookup(relational_operator op,
                             data_view rhs) const override {
    auto do_lookup = [this](relational_operator op,
                         data_view xv) -> caf::optional<bool> {
      if (auto x = caf::get_if<view<T>>(&xv))
        return {lookup_impl(op, *x)};
      else
        return caf::none;
    };
    auto membership = [&]() -> caf::optional<bool> {
      if (auto xs = caf::get_if<view<set>>(&rhs)) {
        for (auto x : **xs) {
          auto result = do_lookup(equal, x);
          if (result && *result)
            return true;
        }
        return false;
      }
      return caf::none;
    };
    switch (op) {
      case in:
        return membership();
      case not_in:
        if (auto result = membership())
          return !*result;
        else
          return result;
      case equal:
      case not_equal:
      case less:
      case less_equal:
      case greater:
      case greater_equal:
        return do_lookup(op, rhs);
      default:
        return caf::none;
    }
  }

  caf::error serialize(caf::serializer& sink) const override {
    return sink(min_, max_);
  }

  caf::error deserialize(caf::deserializer& source) override {
    return source(min_, max_);
  }

  T min() const noexcept {
    return min_;
  }

  T max() const noexcept {
    return max_;
  }

private:
  bool lookup_impl(relational_operator op, const T x) const {
    // Let *min* and *max* constitute the LHS of the lookup operation and *rhs*
    // be the value to compare with on the RHS. Then, there are 5 possible
    // scenarios to differentiate for the inputs:
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
    switch (op) {
      default:
        VAST_ASSERT(!"unsupported operator");
        return false;
      case equal:
        return min_ <= x && x <= max_;
      case not_equal:
        return !(min_ <= x && x <= max_);
      case less:
        return min_ < x;
      case less_equal:
        return min_ <= x;
      case greater:
        return max_ > x;
      case greater_equal:
        return max_ >= x;
    }
  }

  T min_;
  T max_;
};

} // namespace vast
