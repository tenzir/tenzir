//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/legacy_deserialize.hpp"
#include "vast/synopsis.hpp"

namespace vast {

/// A synopsis structure that keeps track of the minimum and maximum value.
template <class T>
class min_max_synopsis : public synopsis {
public:
  min_max_synopsis(vast::type x, T min = T{}, T max = T{})
    : synopsis{std::move(x)}, min_{min}, max_{max} {
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

  [[nodiscard]] std::optional<bool>
  lookup(relational_operator op, data_view rhs) const override {
    auto do_lookup
      = [this](relational_operator op, data_view xv) -> std::optional<bool> {
      if (auto x = caf::get_if<view<T>>(&xv))
        return {lookup_impl(op, *x)};
      else
        return {};
    };
    auto membership = [&]() -> std::optional<bool> {
      if (auto xs = caf::get_if<view<list>>(&rhs)) {
        for (auto x : **xs) {
          auto result = do_lookup(relational_operator::equal, x);
          if (result && *result)
            return true;
        }
        return false;
      }
      return {};
    };
    switch (op) {
      case relational_operator::in:
        return membership();
      case relational_operator::not_in:
        if (auto result = membership())
          return !*result;
        else
          return result;
      case relational_operator::equal:
      case relational_operator::not_equal:
      case relational_operator::less:
      case relational_operator::less_equal:
      case relational_operator::greater:
      case relational_operator::greater_equal:
        return do_lookup(op, rhs);
      default:
        return {};
    }
  }

  [[nodiscard]] size_t memusage() const override {
    return sizeof(min_max_synopsis);
  }

  caf::error inspect_impl(supported_inspectors& inspector) override {
    return std::visit(
      [this](auto inspector) {
        return inspector(min_, max_);
      },
      inspector);
  }

  bool deserialize(vast::detail::legacy_deserializer& source) override {
    return source(min_, max_);
  }

  [[nodiscard]] T min() const noexcept {
    return min_;
  }

  [[nodiscard]] T max() const noexcept {
    return max_;
  }

private:
  [[nodiscard]] bool lookup_impl(relational_operator op, const T x) const {
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
      case relational_operator::equal:
        return min_ <= x && x <= max_;
      case relational_operator::not_equal:
        return !(min_ <= x && x <= max_);
      case relational_operator::less:
        return min_ < x;
      case relational_operator::less_equal:
        return min_ <= x;
      case relational_operator::greater:
        return max_ > x;
      case relational_operator::greater_equal:
        return max_ >= x;
    }
  }

  T min_;
  T max_;
};

} // namespace vast
