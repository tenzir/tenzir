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

#include <caf/detail/pretty_type_name.hpp>

#include <utility>
#include <variant>

namespace tenzir {

/// The return type of `std::forward_like<T>(u)`.
template <class T, class U>
using forward_like_t = decltype(std::forward_like<T>(std::declval<U>()));

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
  = caf::detail::is_complete<variant_traits<std::remove_cvref_t<T>>>
    and requires(const std::remove_cvref_t<T>& v) {
          {
            variant_traits<std::remove_cvref_t<T>>::count
          } -> std::same_as<const std::size_t&>;
          {
            variant_traits<std::remove_cvref_t<T>>::index(v)
          } -> std::same_as<std::size_t>;
          variant_traits<std::remove_cvref_t<T>>::template get<0>(v);
        };

namespace detail {

template <class T, auto accessor>
class auto_variant_traits {
  using underlying = std::remove_cvref_t<decltype(std::invoke(
    accessor, std::declval<const T*>()))>;
  using backing_traits = variant_traits<underlying>;

public:
  constexpr static size_t count = backing_traits::count;

  constexpr static auto index(const T& t) -> size_t {
    return backing_traits::index(std::invoke(accessor, &t));
  }

  template <size_t I>
  constexpr static auto get(const T& t) -> decltype(auto) {
    return backing_traits::template get<I>(std::invoke(accessor, &t));
  }
};

template <class T, auto accessor>
class auto_variant_traits_get_data {
  using underlying
    = std::remove_cvref_t<decltype(std::declval<const T&>().get_data())>;
  using backing_traits = variant_traits<underlying>;

public:
  constexpr static size_t count = backing_traits::count;

  constexpr static auto index(const T& t) -> size_t {
    return backing_traits::index(t.get_data());
  }

  template <size_t I>
  constexpr static auto get(const T& t) -> decltype(auto) {
    return backing_traits::template get<I>(t.get_data());
  }
};

// Intentionally not defined.
template <class... Ts>
auto as_std_variant(const std::variant<Ts...>& x) -> std::variant<Ts...>;

} // namespace detail

template <concepts::unqualified T>
  requires(
    std::same_as<decltype(detail::as_std_variant(std::declval<const T&>()), 0),
                 int>)
class variant_traits<T> {
public:
  static constexpr auto count
    = std::variant_size_v<decltype(detail::as_std_variant(
      std::declval<const T&>()))>;

  static constexpr auto index(const T& x) -> size_t {
    return x.index();
  }

  template <size_t I>
  static constexpr auto get(const T& x) -> decltype(auto) {
    return *std::get_if<I>(&x);
  }
};

template <class T>
  requires has_variant_traits<T>
class variant_traits<std::reference_wrapper<T>>
  : public detail::auto_variant_traits<std::reference_wrapper<T>,
                                       &std::reference_wrapper<T>::get> {};

static_assert(has_variant_traits<std::reference_wrapper<std::variant<int>>>);

namespace detail {
/// Dispatches to `variant_traits<V>::get` and also transfers qualifiers.
template <size_t I, has_variant_traits V>
constexpr auto variant_get(V&& v) -> decltype(auto) {
  static_assert(
    std::is_reference_v<
      decltype(variant_traits<std::remove_cvref_t<V>>::template get<I>(v))>);
  // We call `as_mutable` here because `forward_like` never removes `const`.
  return std::forward_like<V>(
    as_mutable(variant_traits<std::remove_cvref_t<V>>::template get<I>(v)));
}

template <has_variant_traits V, size_t I>
using variant_alternative = std::remove_cvref_t<
  decltype(variant_traits<std::remove_cvref_t<V>>::template get<I>(
    std::declval<V>()))>;

template <has_variant_traits V, class T>
constexpr auto variant_index = std::invoke(
  []<size_t... Is>(std::index_sequence<Is...>) {
    constexpr auto arr
      = std::array{std::same_as<variant_alternative<V, Is>, T>...};
    constexpr auto occurrence_count
      = std::count(std::begin(arr), std::end(arr), true);
    static_assert(occurrence_count == 1,
                  "variant must contain exactly one copy of T");
    return std::distance(std::begin(arr),
                         std::find(arr.begin(), arr.end(), true));
  },
  std::make_index_sequence<variant_traits<V>::count>());

// Ensures that `F` can be invoked with alternative `I` in `V`, yielding type `R`
template <class F, class V, class R, size_t I>
concept variant_invocable_for_r = requires(F f, V v) {
  { std::invoke(f, variant_get<I>(std::forward<V>(v))) } -> std::same_as<R>;
};

/// Ensures that `F` can be invoked with all alternatives `[0..Variant_Size)`
/// in `V`, yielding type `R`
template <class F, class V, class R, size_t Variant_Size>
concept variant_invocable_for_all
  // This is implemented as a hand rolled version, because recursive concepts
  // are illegal and an implementation via fold expressions does not allow the
  // compiler to point at the alternative that failed.
  = (Variant_Size < 33 // Concept is only implemented up to 32 alternatives. If
                       // you ever need more, just add cases at the bottom
     and ((Variant_Size < 1 or variant_invocable_for_r<F, V, R, 0>)
          and (Variant_Size < 2 or variant_invocable_for_r<F, V, R, 1>)
          and (Variant_Size < 3 or variant_invocable_for_r<F, V, R, 2>)
          and (Variant_Size < 4 or variant_invocable_for_r<F, V, R, 3>)
          and (Variant_Size < 5 or variant_invocable_for_r<F, V, R, 4>)
          and (Variant_Size < 6 or variant_invocable_for_r<F, V, R, 5>)
          and (Variant_Size < 7 or variant_invocable_for_r<F, V, R, 6>)
          and (Variant_Size < 8 or variant_invocable_for_r<F, V, R, 7>)
          and (Variant_Size < 9 or variant_invocable_for_r<F, V, R, 8>)
          and (Variant_Size < 10 or variant_invocable_for_r<F, V, R, 9>)
          and (Variant_Size < 11 or variant_invocable_for_r<F, V, R, 10>)
          and (Variant_Size < 12 or variant_invocable_for_r<F, V, R, 11>)
          and (Variant_Size < 13 or variant_invocable_for_r<F, V, R, 12>)
          and (Variant_Size < 14 or variant_invocable_for_r<F, V, R, 13>)
          and (Variant_Size < 15 or variant_invocable_for_r<F, V, R, 14>)
          and (Variant_Size < 16 or variant_invocable_for_r<F, V, R, 15>)
          and (Variant_Size < 17 or variant_invocable_for_r<F, V, R, 16>)
          and (Variant_Size < 18 or variant_invocable_for_r<F, V, R, 17>)
          and (Variant_Size < 19 or variant_invocable_for_r<F, V, R, 18>)
          and (Variant_Size < 20 or variant_invocable_for_r<F, V, R, 19>)
          and (Variant_Size < 21 or variant_invocable_for_r<F, V, R, 20>)
          and (Variant_Size < 22 or variant_invocable_for_r<F, V, R, 21>)
          and (Variant_Size < 24 or variant_invocable_for_r<F, V, R, 22>)
          and (Variant_Size < 25 or variant_invocable_for_r<F, V, R, 23>)
          and (Variant_Size < 26 or variant_invocable_for_r<F, V, R, 24>)
          and (Variant_Size < 27 or variant_invocable_for_r<F, V, R, 25>)
          and (Variant_Size < 28 or variant_invocable_for_r<F, V, R, 26>)
          and (Variant_Size < 29 or variant_invocable_for_r<F, V, R, 27>)
          and (Variant_Size < 30 or variant_invocable_for_r<F, V, R, 29>)
          and (Variant_Size < 31 or variant_invocable_for_r<F, V, R, 30>)
          and (Variant_Size < 32 or variant_invocable_for_r<F, V, R, 31>)));

// Ensures that the Functor `F` can be invoked with every alternative in `V`,
// yielding the same type for all alternatives
template <class F, class V>
concept variant_matcher_for
  = has_variant_traits<V>
    and requires(F f, V v) { variant_get<0>(v); }
    /// The return type of invoking the functor with the first alternative
    /// determines the final return type
    and requires(F f, V v) { f(variant_get<0>(v)); }
    /// Invoking the functor with all of the alternatives must yield the same
    /// type
    and variant_invocable_for_all<
      F, V, std::invoke_result_t<F, decltype(variant_get<0>(std::declval<V>()))>,
      variant_traits<std::remove_cvref_t<V>>::count>;

template <class V, variant_matcher_for<V> F>
constexpr auto match_one(V&& v, F&& f) -> decltype(auto) {
  using traits = variant_traits<std::remove_cvref_t<V>>;
  using return_type = std::invoke_result_t<F, decltype(variant_get<0>(v))>;
  auto index = traits::index(std::as_const(v));
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
          return static_cast<return_type>(std::invoke(
            std::forward<F>(f), variant_get<Is>(std::forward<V>(v))));
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

/// Calls one of the given functions with the current variant alternative.
/// This overload takes by value, as it makes a copy internally.
/// If you need your matcher to be a reference, you can pass it as a `std::ref`.
template <has_variant_traits V, class... Fs>
constexpr auto match(V&& v, Fs... fs) -> decltype(auto) {
  return detail::match_one(std::forward<V>(v),
                           detail::overload{std::move(fs)...});
}

/// Calls the given functions with the current variant alternative.
template <has_variant_traits V, class F>
constexpr auto match(V&& v, F&& f) -> decltype(auto) {
  return detail::match_one(std::forward<V>(v), std::forward<F>(f));
}

/// Calls one of the given functions with the current variant alternatives.
/// This overload takes by value, as it makes a copy internally.
/// If you need your matcher to be a reference, you can pass it as a `std::ref`.
template <has_variant_traits... Ts, class... Fs>
constexpr auto match(std::tuple<Ts...> v, Fs... fs) -> decltype(auto) {
  return detail::match_tuple(std::move(v), detail::overload{std::move(fs)...});
}

/// Calls the given function with the current variant alternatives
template <has_variant_traits... Ts, class F>
constexpr auto match(std::tuple<Ts...> v, F&& f) -> decltype(auto) {
  return detail::match_tuple(std::move(v), std::forward<F>(f));
}

/// Checks whether the variant currently holds alternative `T`
template <concepts::unqualified T, has_variant_traits V>
auto is(const V& v) -> bool {
  using bare = std::remove_cvref_t<V>;
  constexpr auto alternative_index = detail::variant_index<bare, T>;
  const auto current_index = variant_traits<bare>::index(v);
  return current_index == alternative_index;
}

/// Extracts a `T` from the given variant, asserting success.
template <concepts::unqualified T, has_variant_traits V>
auto as(V&& v) -> forward_like_t<V, T> {
  using bare = std::remove_cvref_t<V>;
  constexpr auto alternative_index = detail::variant_index<bare, T>;
  const auto current_index = variant_traits<bare>::index(v);
  TENZIR_ASSERT(current_index == alternative_index,
                "invalid variant access: [current: `{} ({})`] != [requested: "
                "`{} ({})`]",
                current_index,
                match(std::forward<V>(v),
                      []<typename H>(const H&) {
                        return caf::detail::pretty_type_name(
                          typeid(std::remove_cvref_t<H>));
                      }),
                alternative_index, caf::detail::pretty_type_name(typeid(T)));
  return detail::variant_get<alternative_index>(std::forward<V>(v));
};

/// Tries to extract a `T` from the variant, returning `nullptr` otherwise.
template <concepts::unqualified T, has_variant_traits V>
auto try_as(V& v) -> std::remove_reference_t<forward_like_t<V, T>>* {
  using bare = std::remove_cvref_t<V>;
  constexpr auto alternative_index = detail::variant_index<bare, T>;
  const auto current_index = variant_traits<bare>::index(v);
  if (current_index != alternative_index) {
    return nullptr;
  }
  return &detail::variant_get<alternative_index>(v);
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
