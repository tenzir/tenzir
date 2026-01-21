//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/variant.hpp"

namespace tenzir {

template <typename T, typename = void>
struct IsGenericLambda : std::true_type {
  static_assert(std::same_as<std::remove_cvref_t<T>, T>);
};

template <typename T>
struct IsGenericLambda<T, std::void_t<decltype(&T::operator())>>
  : std::false_type {};

template <class T, class... Fs>
constexpr auto index_for() -> size_t {
  auto invocable = std::array<bool, sizeof...(Fs)>{std::invocable<Fs, T>...};
  auto count = size_t{0};
  for (auto& x : invocable) {
    if (x) {
      count += 1;
    }
  }
  if (count == 1) {
    auto index = size_t{0};
    for (auto& x : invocable) {
      if (x) {
        return index;
      }
      index += 1;
    }
    __builtin_unreachable();
  }
  if (count == 0) {
    throw std::runtime_error("could not find any handler for T");
  }
  // Reduce it to non-generic lambdas.
  invocable = {(std::invocable<Fs, T>
                and not IsGenericLambda<std::remove_cvref_t<Fs>>::value)...};
  count = 0;
  for (auto& x : invocable) {
    if (x) {
      count += 1;
    }
  }
  if (count == 1) {
    auto index = size_t{0};
    for (auto& x : invocable) {
      if (x) {
        return index;
      }
      index += 1;
    }
    __builtin_unreachable();
  }
  if (count == 0) {
    throw std::runtime_error(
      "found multiple generic handlers and no non-generic handler");
  }
  throw std::runtime_error("found multiple non-generic handlers");
}

// TODO: We just return result for first check. Could do more.
template <has_variant_traits V, class... Fs>
using CoMatch = std::invoke_result_t<
  std::tuple_element_t<
    index_for<decltype(detail::variant_get<0>(std::declval<V>())), Fs...>(),
    std::tuple<Fs...>>,
  decltype(detail::variant_get<0>(std::declval<V>()))>;

template <has_variant_traits V, class... Fs>
constexpr auto co_match(V&& v, Fs&&... fs) -> CoMatch<V, Fs...> {
  constexpr auto count = variant_traits<std::remove_cvref_t<V>>::count;
  auto index = variant_traits<std::remove_cvref_t<V>>::index(v);
  TENZIR_ASSERT(index < count);
#define X(n)                                                                   \
  if constexpr ((n) < count) {                                                 \
    if (index == (n)) {                                                        \
      constexpr auto index                                                     \
        = index_for<decltype(detail::variant_get<n>(std::forward<V>(v))),      \
                    Fs...>();                                                  \
      return std::invoke(                                                      \
        std::get<index>(std::forward_as_tuple(std::forward<Fs>(fs)...)),       \
        detail::variant_get<n>(std::forward<V>(v)));                           \
    }                                                                          \
  }
  X(0);
  X(1);
  X(2);
  X(3);
  X(4);
  X(5);
  X(6);
  X(7);
  X(8);
  X(9);
  X(10);
  X(11);
  X(12);
  X(13);
  X(14);
  X(15);
#undef X
  static_assert(count <= 16);
  __builtin_unreachable();
}

} // namespace tenzir
