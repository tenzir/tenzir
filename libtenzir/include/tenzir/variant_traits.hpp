//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concepts.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/overload.hpp"

#include <caf/variant.hpp>

#include <variant>

namespace tenzir {

/// A backport of C++23's `forward_like`.
template <class T, class U>
constexpr auto forward_like(U&& x) noexcept -> auto&& {
  constexpr bool is_adding_const = std::is_const_v<std::remove_reference_t<T>>;
  if constexpr (std::is_lvalue_reference_v<T&&>) {
    if constexpr (is_adding_const) {
      return std::as_const(x);
    } else {
      return static_cast<U&>(x);
    }
  } else {
    if constexpr (is_adding_const) {
      return std::move(std::as_const(x));
    } else {
      return std::move(x);
    }
  }
}

/// The return type of `std::forward_like<T>(u)`.
template <class T, class U>
using forward_like_t = decltype(forward_like<T>(std::declval<U>()));

/// The opposite of `std::as_const`, removing `const` qualifiers.
template <class T>
auto as_mutable(T& x) -> T& {
  return x;
}
template <class T>
auto as_mutable(const T& x) -> T& {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  return const_cast<T&>(x);
}
template <class T>
auto as_mutable(T&& x) -> T&& {
  return std::forward<T>(x);
}
template <class T>
auto as_mutable(const T&& x) -> T&& {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  return const_cast<T&&>(x);
}

/// Enables variant methods (like `match`) for a given type.
///
/// Implementations need to provide the following `static` members:
/// - `constexpr size_t count`
/// - `index(const T&) -> size_t`
/// - `get<size_t>(const T&) -> const U&`
///
/// The `index` function may only return indicies in `[0, count)`. The `get`
/// function is instantiated for all `[0, count)` and may assume that the
/// given index is what `index(...)` previously returned. If the original
/// object was `T&` or `T&&`, the implementation will `const_cast` the result
/// of `get(...)`.
template <concepts::unqualified T>
class variant_traits;

template <class T>
concept has_variant_traits
  = caf::detail::is_complete<variant_traits<std::remove_cvref_t<T>>>;

template <class... Ts>
class variant_traits<std::variant<Ts...>> {
public:
  static constexpr auto count = sizeof...(Ts);

  static constexpr auto index(const std::variant<Ts...>& x) -> size_t {
    return x.index();
  }

  template <size_t I>
  static constexpr auto get(const std::variant<Ts...>& x) -> decltype(auto) {
    return *std::get_if<I>(&x);
  }
};

template <class... Ts>
class variant_traits<caf::variant<Ts...>> {
public:
  static constexpr auto count = sizeof...(Ts);

  static auto index(const caf::variant<Ts...>& x) -> size_t {
    return x.index();
  }

  template <size_t I>
  static auto get(const caf::variant<Ts...>& x) -> decltype(auto) {
    using T = std::tuple_element_t<I, std::tuple<Ts...>>;
    return *caf::sum_type_access<caf::variant<Ts...>>::get_if(
      &x, caf::sum_type_token<T, I>{});
  }
};

namespace detail {

/// Dispatches to `variant_traits<V>::get` and also transfers qualifiers.
template <size_t I, has_variant_traits V>
constexpr auto variant_get(V&& v) -> decltype(auto) {
  static_assert(
    std::is_reference_v<
      decltype(variant_traits<std::remove_cvref_t<V>>::template get<I>(v))>);
  // We call `as_mutable` here because `forward_like` never removes `const`.
  return forward_like<V>(
    as_mutable(variant_traits<std::remove_cvref_t<V>>::template get<I>(v)));
}

template <has_variant_traits V, size_t I>
using variant_alternative = std::remove_cvref_t<
  decltype(variant_traits<std::remove_cvref_t<V>>::template get<I>(
    std::declval<V>()))>;

template <has_variant_traits V, class T>
constexpr auto variant_index = std::invoke(
  []<size_t... Is>(std::index_sequence<Is...>) {
    constexpr bool arr[] = {std::same_as<variant_alternative<V, Is>, T>...};
    constexpr auto occurrence_count
      = std::count(std::begin(arr), std::end(arr), true);
    static_assert(occurrence_count == 1,
                  "variant must contain exactly one copy of T");
    return std::distance(std::begin(arr), std::ranges::find(arr, true));
  },
  std::make_index_sequence<variant_traits<V>::count>());

// Ensures that `F` can be invoked with alternative `I` in `V`, yielding result
// type `R`
template <class F, class V, class R, size_t I>
concept variant_invocable_for_r = requires(F f, V v) {
  requires std::invocable<F, decltype(variant_get<I>(std::forward<V>(v)))>;
  // requires std::is_invocable_r_v<R, F, decltype(variant_get<I>(v))>;
};

template <class F, class V, class R, size_t... Is>
consteval auto check_matcher(std::index_sequence<Is...>) {
  return (variant_invocable_for_r<F, V, R, Is> && ...);
}

// Ensures that the Functor `F` can be invoked with every alternative in `V`,
// respecting value categories
template <class F, class V>
concept variant_matcher_for = has_variant_traits<V> and requires {
  typename std::invoke_result_t<F, decltype(variant_get<0>(std::declval<V>()))>;
  requires check_matcher<
    F, V, std::invoke_result_t<F, decltype(variant_get<0>(std::declval<V>()))>>(
    std::make_index_sequence<variant_traits<std::remove_cvref_t<V>>::count>{});
};

template <class V, variant_matcher_for<V> F>
constexpr auto match_one(V&& v, F&& f) -> decltype(auto) {
  using traits = variant_traits<std::remove_cvref_t<V>>;
  using return_type = std::invoke_result_t<F, decltype(variant_get<0>(v))>;
  auto index = traits::index(std::as_const(v));
  static_assert(std::same_as<decltype(index), size_t>);
  // TODO: A switch/if-style dispatch might be more performant.
  constexpr auto table = std::invoke(
    []<size_t... Is>(std::index_sequence<Is...>) {
      return std::array{
        // Arguments are not necessarily &&-refs due to reference collapsing.
        // NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved)
        +[](F&& f, V&& v) -> return_type {
          // We repeat ourselves here because we don't want to handle the case
          // where `void` is returned separately.
          using local_return_type = decltype(std::invoke(
            std::forward<F>(f), variant_get<Is>(std::forward<V>(v))));
          static_assert(std::same_as<local_return_type, return_type>,
                        "all cases must have the same return type");
          return std::invoke(std::forward<F>(f),
                             variant_get<Is>(std::forward<V>(v)));
        }...,
      };
    },
    std::make_index_sequence<traits::count>());
  static_assert(table.size() == traits::count);
  TENZIR_ASSERT(index < traits::count);
  return table[index](std::forward<F>(f), std::forward<V>(v)); // NOLINT
}

template <class... Xs, class F>
constexpr auto match_tuple(std::tuple<Xs...> xs, F&& f) -> decltype(auto) {
  // There are probably more performant ways to write this, but the
  // implementation below should be good enough for now.
  if constexpr (sizeof...(Xs) == 0) {
    return f();
  } else {
    auto&& x = std::get<0>(xs);
    return match_one(std::forward<decltype(x)>(x),
                     [&]<class X>(X&& x) -> decltype(auto) {
                       return match_tuple(
                         // Strip the first value from the tuple.
                         std::apply(
                           []<class... Ys>(auto&&, Ys&&... ys) {
                             return std::tuple<Ys&&...>{ys...};
                           },
                           xs),
                         // Then combine the already matched `x` with the rest.
                         [&]<class... Ys>(Ys&&... ys) -> decltype(auto) {
                           return std::forward<F>(f)(std::forward<X>(x),
                                                     std::forward<Ys>(ys)...);
                         });
                     });
  }
}
} // namespace detail

/// Calls one of the given functions with the current variant inhabitant.
template <has_variant_traits V, class... Fs>
constexpr auto match(V&& v, Fs&&... fs) -> decltype(auto) {
  return match_one(std::forward<V>(v),
                   detail::overload{std::forward<Fs>(fs)...});
}

/// Calls one of the given functions with the current variant inhabitants.
template <has_variant_traits... Ts, class... Fs>
constexpr auto match(std::tuple<Ts...> v, Fs&&... fs) -> decltype(auto) {
  return match_tuple(std::move(v), detail::overload{std::forward<Fs>(fs)...});
}

/// Extracts a `T` from the given variant, asserting success.
template <concepts::unqualified T, has_variant_traits V>
auto as(V&& v) -> forward_like_t<V, T> {
  constexpr auto index = detail::variant_index<std::remove_cvref_t<V>, T>;
  TENZIR_ASSERT(variant_traits<std::remove_cvref_t<V>>::index(v) == index);
  return detail::variant_get<index>(std::forward<V>(v));
};

/// Tries to extract a `T` from the variant, returning `nullptr` otherwise.
template <concepts::unqualified T, has_variant_traits V>
auto try_as(V& v) -> std::remove_reference_t<forward_like_t<V, T>>* {
  constexpr auto index = detail::variant_index<std::remove_const_t<V>, T>;
  if (variant_traits<std::remove_const_t<V>>::index(v) != index) {
    return nullptr;
  }
  return &detail::variant_get<index>(v);
};
/// Tries to extract a `T` from the variant, returning `nullptr` otherwise.
template <concepts::unqualified T, has_variant_traits V>
auto try_as(V* v) -> std::remove_reference_t<forward_like_t<V, T>>* {
  if (not v) {
    return nullptr;
  }
  return try_as<T>(*v);
};

} // namespace tenzir
