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

// Extract the first parameter type from a member function pointer.
template <typename>
struct FirstParamType {};

template <typename R, typename C, typename Arg, typename... Args>
struct FirstParamType<R (C::*)(Arg, Args...)> {
  using type = Arg;
};

template <typename R, typename C, typename Arg, typename... Args>
struct FirstParamType<R (C::*)(Arg, Args...) const> {
  using type = Arg;
};

template <typename R, typename C, typename Arg, typename... Args>
struct FirstParamType<R (C::*)(Arg, Args...) noexcept> {
  using type = Arg;
};

template <typename R, typename C, typename Arg, typename... Args>
struct FirstParamType<R (C::*)(Arg, Args...) const noexcept> {
  using type = Arg;
};

// Check if a callable F has an exact match for type T (not just convertible).
template <typename T, typename F, typename = void>
struct IsExactMatch : std::false_type {};

template <typename T, typename F>
struct IsExactMatch<
  T, F, std::void_t<typename FirstParamType<decltype(&F::operator())>::type>> {
  using ParamType = typename FirstParamType<decltype(&F::operator())>::type;
  static constexpr bool value
    = std::same_as<std::remove_cvref_t<T>, std::remove_cvref_t<ParamType>>;
};

namespace detail {

template <class... Fs>
constexpr auto index_for_impl(std::array<bool, sizeof...(Fs)> invocable)
  -> size_t {
  auto count = size_t{0};
  for (auto x : invocable) {
    if (x) {
      count += 1;
    }
  }
  if (count == 1) {
    auto index = size_t{0};
    for (auto x : invocable) {
      if (x) {
        return index;
      }
      index += 1;
    }
    __builtin_unreachable();
  }
  if (count == 0) {
    return size_t(-1); // No match found
  }
  return size_t(-2); // Multiple matches found
}

} // namespace detail

template <class T, class... Fs>
constexpr auto index_for() -> size_t {
  auto invocable = std::array<bool, sizeof...(Fs)>{std::invocable<Fs, T>...};
  if (auto r = detail::index_for_impl<Fs...>(invocable); r < sizeof...(Fs)) {
    return r;
  } else if (r == size_t(-1)) {
    throw std::runtime_error("could not find any handler for T");
  }
  // Reduce it to non-generic lambdas.
  invocable = {(std::invocable<Fs, T>
                and not IsGenericLambda<std::remove_cvref_t<Fs>>::value)...};
  if (auto r = detail::index_for_impl<Fs...>(invocable); r < sizeof...(Fs)) {
    return r;
  } else if (r == size_t(-1)) {
    throw std::runtime_error(
      "found multiple generic handlers and no non-generic handler");
  }
  // Reduce it to exact matches (not just convertible).
  invocable = {(std::invocable<Fs, T>
                and not IsGenericLambda<std::remove_cvref_t<Fs>>::value
                and IsExactMatch<T, std::remove_cvref_t<Fs>>::value)...};
  if (auto r = detail::index_for_impl<Fs...>(invocable); r < sizeof...(Fs)) {
    return r;
  } else if (r == size_t(-1)) {
    throw std::runtime_error(
      "found multiple non-generic handlers and no exact match");
  }
  throw std::runtime_error("found multiple exact-match handlers");
}

// Multi-argument version of index_for, used for tuple co_match.
// Returns the index via invoke() - no static member to avoid eager evaluation.
template <class ArgsTuple, class... Fs>
struct index_for_multi;

template <class... Args, class... Fs>
struct index_for_multi<std::tuple<Args...>, Fs...> {
  static constexpr auto invoke() -> size_t {
    auto invocable
      = std::array<bool, sizeof...(Fs)>{std::invocable<Fs, Args...>...};
    if (auto r = detail::index_for_impl<Fs...>(invocable); r < sizeof...(Fs)) {
      return r;
    } else if (r == size_t(-1)) {
      throw std::runtime_error("could not find any handler for Args...");
    }
    // Reduce it to non-generic lambdas.
    invocable = {(std::invocable<Fs, Args...>
                  and not IsGenericLambda<std::remove_cvref_t<Fs>>::value)...};
    if (auto r = detail::index_for_impl<Fs...>(invocable); r < sizeof...(Fs)) {
      return r;
    } else if (r == size_t(-1)) {
      throw std::runtime_error(
        "found multiple generic handlers and no non-generic handler");
    }
    // For multi-arg, we skip exact match checking (would need to extend
    // IsExactMatch). If we reach here, we have multiple non-generic handlers.
    throw std::runtime_error("found multiple non-generic handlers");
  }
};

// Type tag for passing type lists without instantiation.
template <class... Ts>
struct type_list {};

// Helper to check if a handler at index I is the best match for given args.
// Uses a constexpr function to make evaluation lazy (only when the requires
// clause is checked), avoiding hard errors for type combinations that won't
// be used.
template <class FsTypeList, class ArgsTuple, size_t I>
struct is_best_handler_for;

template <class... Fs, class ArgsTuple, size_t I>
struct is_best_handler_for<type_list<Fs...>, ArgsTuple, I> {
  static constexpr auto check() -> bool {
    return index_for_multi<ArgsTuple, Fs...>::invoke() == I;
  }
};

// TODO: We just return result for first check. Could do more.
template <has_variant_traits V, class... Fs>
using CoMatch = std::invoke_result_t<
  std::tuple_element_t<
    index_for<decltype(detail::variant_get<0>(std::declval<V>())), Fs...>(),
    std::tuple<Fs...>>,
  decltype(detail::variant_get<0>(std::declval<V>()))>;

/// Invokes a callable depending on the current variant inhabitant.
///
/// Currently used for functions returning coroutines, but this will eventually
/// just replace the current `match` function.
///
/// If multiple callables are invocable, we use this order:
/// 1) Exact match, where the argument type is the same as the inhabitant.
/// 2) Non-generic match, where the callable is not a template.
/// 3) All functions that can be invoked with the inhabitant.
/// If there is not a unique-best match, a compile-time error occurs.
///
/// Unlike `match`, the callables are not required to be movable or copyable, as
/// they are simply used as references. Due to lifetime rules around
/// subexpressions, this makes the `co_await co_match(…)` patterns safe.
template <has_variant_traits V, class... Fs>
constexpr auto co_match(V&& v, Fs&&... fs) -> CoMatch<V, Fs...> {
  constexpr auto count = variant_traits<std::remove_cvref_t<V>>::count;
  auto index = variant_traits<std::remove_cvref_t<V>>::index(v);
  TENZIR_ASSERT(index < count);
  // This is intentionally not a table as the assumption is that the optimizer
  // can deal with this better. Didn't really confirm this yet though.
#define X(n)                                                                   \
  if constexpr ((n) < count) {                                                 \
    if (index == (n)) {                                                        \
      constexpr auto index                                                     \
        = index_for<decltype(detail::variant_get<n>(std::forward<V>(v))),      \
                    Fs...>();                                                  \
      using Result = decltype(std::invoke(                                     \
        std::get<index>(std::forward_as_tuple(std::forward<Fs>(fs)...)),       \
        detail::variant_get<n>(std::forward<V>(v))));                          \
      static_assert(std::same_as<Result, CoMatch<V, Fs...>>,                   \
                    "inconsistent return type in match");                      \
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

// Implementation helper for tuple co_match with index tracking.
template <has_variant_traits... Vs, class FsTuple, class FsTypeList,
          size_t... Is>
constexpr auto co_match_tuple_impl(std::tuple<Vs...>&& vs, FsTuple&& fs_tuple,
                                   FsTypeList, std::index_sequence<Is...>)
  -> decltype(auto) {
  // We do create temporary lambdas here that are destroyed before the coroutine
  // potentially returned by the supplied functions is done. However, our
  // lambdas themselves are not coroutines, and they do not pass any state owned
  // by them into the user-supplied coroutines.
  return co_match(
    std::get<0>(std::move(vs)), [&]<class X>(X&& x) -> decltype(auto) {
      return co_match(
        // Remove the first value from the tuple. Use std::move to preserve
        // rvalue-ness of tuple elements (otherwise reference collapsing
        // turns rvalue refs into lvalue refs).
        std::apply(
          []<class... Ws>(auto&&, Ws&&... ws) {
            return std::tuple<Ws&&...>{std::forward<Ws>(ws)...};
          },
          std::move(vs)),
        // Create wrappers with constraints using the original Fs for
        // disambiguation. Each wrapper is only enabled when its corresponding
        // handler is the best match according to index_for_multi.
        // Note: We use SFINAE via enable_if_t on the return type instead of
        // a requires clause to work around GCC 15 issues with evaluating
        // requires clauses that reference template parameters from enclosing
        // scopes during template substitution.
        [&]<class... Xs>(Xs&&... xs)
          -> std::enable_if_t<
            is_best_handler_for<FsTypeList, std::tuple<X, Xs...>, Is>::check(),
            decltype(std::invoke(std::get<Is>(fs_tuple), std::forward<X>(x),
                                 std::forward<Xs>(xs)...))> {
          return std::invoke(std::get<Is>(fs_tuple), std::forward<X>(x),
                             std::forward<Xs>(xs)...);
        }...);
    });
}

template <has_variant_traits... Vs, class... Fs>
constexpr auto co_match(std::tuple<Vs...>&& vs, Fs&&... fs) -> decltype(auto) {
  static_assert(sizeof...(Vs) > 0);
  if constexpr (sizeof...(Vs) == 1) {
    return co_match(std::get<0>(std::move(vs)), std::forward<Fs>(fs)...);
  } else {
    // Use a type list of the original Fs for disambiguation in wrappers.
    using FsTypeList = type_list<std::remove_cvref_t<Fs>...>;
    return co_match_tuple_impl(std::move(vs),
                               std::forward_as_tuple(std::forward<Fs>(fs)...),
                               FsTypeList{}, std::index_sequence_for<Fs...>{});
  }
}

} // namespace tenzir
