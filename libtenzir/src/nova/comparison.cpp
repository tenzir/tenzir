//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/comparison.hpp"

#include "tenzir/nova/array.hpp"

#include <cmath>
#include <ranges>
#include <tuple>

namespace tenzir::nova {

namespace {

auto rank(RowView<Data> value) -> size_t {
  return match(value, []<class T>(RowView<T>) {
    if constexpr (std::same_as<T, Bool>) {
      return size_t{0};
    } else if constexpr (std::same_as<T, Int>) {
      return size_t{1};
    } else if constexpr (std::same_as<T, UInt>) {
      return size_t{2};
    } else if constexpr (std::same_as<T, Float>) {
      return size_t{3};
    } else if constexpr (std::same_as<T, Duration>) {
      return size_t{4};
    } else if constexpr (std::same_as<T, Time>) {
      return size_t{5};
    } else if constexpr (std::same_as<T, String>) {
      return size_t{6};
    } else if constexpr (std::same_as<T, Ip>) {
      return size_t{7};
    } else if constexpr (std::same_as<T, Subnet>) {
      return size_t{8};
    } else if constexpr (std::same_as<T, List>) {
      return size_t{9};
    } else if constexpr (std::same_as<T, Record>) {
      return size_t{10};
    } else if constexpr (std::same_as<T, Secret>) {
      return size_t{11};
    } else if constexpr (std::same_as<T, Blob>) {
      return size_t{12};
    } else {
      static_assert(std::same_as<T, Null>);
      return size_t{13};
    }
  });
}

auto apply_order(std::weak_ordering relation, Order order)
  -> std::weak_ordering {
  if (order == Order::ascending) {
    return relation;
  }
  return relation < 0   ? std::weak_ordering::greater
         : relation > 0 ? std::weak_ordering::less
                        : std::weak_ordering::equivalent;
}

} // namespace

auto weak_order(RowView<Data> lhs, RowView<Data> rhs, Order order)
  -> std::weak_ordering {
  return match(
    std::tie(lhs, rhs),
    [order]<class L, class R>(RowView<L> lhs,
                              RowView<R> rhs) -> std::weak_ordering {
      constexpr auto lhs_numeric = concepts::one_of<L, Int, UInt, Float>;
      constexpr auto rhs_numeric = concepts::one_of<R, Int, UInt, Float>;
      if constexpr (lhs_numeric and rhs_numeric) {
        auto const relation = _::compare_numbers(*lhs, *rhs);
        if (relation == std::partial_ordering::unordered) {
          if constexpr (std::same_as<L, Float> and std::same_as<R, Float>) {
            return apply_order(std::isnan(*lhs) == std::isnan(*rhs)
                                 ? std::weak_ordering::equivalent
                               : std::isnan(*lhs) ? std::weak_ordering::greater
                                                  : std::weak_ordering::less,
                               order);
          } else if constexpr (std::same_as<L, Float>) {
            return apply_order(std::weak_ordering::greater, order);
          } else {
            return apply_order(std::weak_ordering::less, order);
          }
        }
        return apply_order(relation < 0   ? std::weak_ordering::less
                           : relation > 0 ? std::weak_ordering::greater
                                          : std::weak_ordering::equivalent,
                           order);
      } else if constexpr (not std::same_as<L, R>) {
        if constexpr (std::same_as<L, Null>) {
          return std::weak_ordering::greater;
        } else if constexpr (std::same_as<R, Null>) {
          return std::weak_ordering::less;
        }
        return apply_order(
          rank(RowView<Data>{lhs}) <=> rank(RowView<Data>{rhs}), order);
      } else if constexpr (std::same_as<L, Null> or std::same_as<L, Secret>) {
        return std::weak_ordering::equivalent;
      } else if constexpr (std::same_as<L, List>) {
        auto lhs_it = lhs.begin();
        auto rhs_it = rhs.begin();
        while (lhs_it != lhs.end() and rhs_it != rhs.end()) {
          if (auto result = weak_order(*lhs_it, *rhs_it, order);
              result != std::weak_ordering::equivalent) {
            return result;
          }
          ++lhs_it;
          ++rhs_it;
        }
        if (lhs_it == lhs.end() and rhs_it == rhs.end()) {
          return std::weak_ordering::equivalent;
        }
        return apply_order(lhs_it == lhs.end() ? std::weak_ordering::less
                                               : std::weak_ordering::greater,
                           order);
      } else if constexpr (std::same_as<L, Record>) {
        auto lhs_it = lhs.begin();
        auto rhs_it = rhs.begin();
        while (lhs_it != lhs.end() and rhs_it != rhs.end()) {
          auto [lhs_name, lhs_value] = *lhs_it;
          auto [rhs_name, rhs_value] = *rhs_it;
          if (auto result = apply_order(lhs_name <=> rhs_name, order);
              result != std::weak_ordering::equivalent) {
            return result;
          }
          if (auto result = weak_order(lhs_value, rhs_value, order);
              result != std::weak_ordering::equivalent) {
            return result;
          }
          ++lhs_it;
          ++rhs_it;
        }
        if (lhs_it == lhs.end() and rhs_it == rhs.end()) {
          return std::weak_ordering::equivalent;
        }
        return apply_order(lhs_it == lhs.end() ? std::weak_ordering::less
                                               : std::weak_ordering::greater,
                           order);
      } else if constexpr (std::same_as<L, Blob>) {
        if (std::ranges::lexicographical_compare(*lhs, *rhs)) {
          return apply_order(std::weak_ordering::less, order);
        }
        if (std::ranges::lexicographical_compare(*rhs, *lhs)) {
          return apply_order(std::weak_ordering::greater, order);
        }
        return std::weak_ordering::equivalent;
      } else {
        if (compare<ast::binary_op::lt>(*lhs, *rhs)) {
          return apply_order(std::weak_ordering::less, order);
        }
        if (compare<ast::binary_op::gt>(*lhs, *rhs)) {
          return apply_order(std::weak_ordering::greater, order);
        }
        return std::weak_ordering::equivalent;
      }
    });
}

} // namespace tenzir::nova
