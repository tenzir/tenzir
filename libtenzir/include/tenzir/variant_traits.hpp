//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/tql2/ast.hpp"
#include "tenzir/type.hpp"

#include <caf/variant.hpp>

#include <variant>

namespace tenzir {

/// Enables variant methods (like `match`) for a given type.
///
/// Implementations need to provide the following `static` members:
/// - `constexpr size_t count`
/// - `index(const T&) -> size_t`
/// - `get<size_t>(const T&) -> const U&`
///
/// The `index` function may only return indicies in `[0, count)`. The `get`
/// function is instantiated for all `[0, count)` and may assume that the given
/// index is what `index(...)` previously returned. If the original object was
/// `T&` or `T&&`, the implementation will `const_cast` the result of `get(...)`.
template <class T>
class variant_traits;

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

template <>
class variant_traits<type> {
public:
  static constexpr auto count = caf::detail::tl_size<concrete_types>::value;

  // TODO: Can we maybe align the indices here?
  static auto index(const type& x) -> size_t;

  template <size_t I>
  static auto get(const type& x) -> decltype(auto) {
    using Type = caf::detail::tl_at_t<concrete_types, I>;
    if constexpr (basic_type<Type>) {
      // TODO: We potentially hand out a `&` to a const... Probably okay because
      // we can't do any mutations, but still fishy.
      static constexpr auto instance = Type{};
      // The ref-deref is needed to deduce it as a reference.
      return *&instance;
    } else {
      // TODO: This might be a violation of the aliasing rules.
      return static_cast<const Type&>(
        static_cast<const stateful_type_base&>(x));
    }
  }
};

/// Requires that the Arrow type has a matching Tenzir type.
auto arrow_type_to_type_variant_index(const arrow::DataType& ty) -> size_t;

template <>
class variant_traits<ast::expression> {
public:
  static constexpr auto count
    = caf::detail::tl_size<ast::expression_kinds>::value;

  static auto index(const ast::expression& x) -> size_t {
    TENZIR_ASSERT(x.kind);
    return x.kind->index();
  }

  template <size_t I>
  static auto get(const ast::expression& x) -> decltype(auto) {
    return *std::get_if<I>(&*x.kind);
  }
};

template <>
class variant_traits<data> {
public:
  using impl = variant_traits<data::variant>;

  static constexpr auto count = impl::count;

  static auto index(const data& x) -> size_t {
    return impl::index(x.get_data());
  }

  template <size_t I>
  static auto get(const data& x) -> decltype(auto) {
    return impl::get<I>(x.get_data());
  }
};

template <>
class variant_traits<arrow::Array> {
public:
  using types = caf::detail::tl_map_t<concrete_types, type_to_arrow_array>;

  static constexpr auto count = caf::detail::tl_size<types>::value;

  static auto index(const arrow::Array& x) -> size_t {
    return arrow_type_to_type_variant_index(*x.type());
  }

  template <size_t I>
  static auto get(const arrow::Array& x) -> decltype(auto) {
    return static_cast<const caf::detail::tl_at_t<types, I>&>(x);
  }
};

template <>
class variant_traits<arrow::ArrayBuilder> {
public:
  using types = caf::detail::tl_map_t<concrete_types, type_to_arrow_builder>;

  static constexpr auto count = caf::detail::tl_size<types>::value;

  static auto index(const arrow::ArrayBuilder& self) -> size_t {
    return arrow_type_to_type_variant_index(*self.type());
  }

  template <size_t I>
  static auto get(const arrow::ArrayBuilder& self) -> decltype(auto) {
    return static_cast<const caf::detail::tl_at_t<types, I>&>(self);
  }
};

/// Copies the ref and const qualifiers from one type to another.
template <class T, class U>
constexpr auto transfer_const_ref(const U& x) -> decltype(auto) {
  // TODO: Check this.
  using TNoRef = std::remove_reference_t<T>;
  using UNoCvRef = std::remove_cvref_t<U>;
  if constexpr (std::is_lvalue_reference_v<T>) {
    if constexpr (std::is_const_v<TNoRef>) {
      return static_cast<std::add_const_t<UNoCvRef>&>(x);
    } else {
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
      return const_cast<std::remove_const_t<UNoCvRef>&>(x);
    }
  } else {
    if constexpr (std::is_const_v<TNoRef>) {
      return static_cast<std::add_const_t<UNoCvRef>&&>(x);
    } else {
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
      return const_cast<std::remove_const_t<UNoCvRef>&&>(x);
    }
  }
}

/// Dispatches to `variant_traits<V>`, but also copies ref and const qualifiers.
template <size_t I, class V>
constexpr auto get_impl(V&& v) -> decltype(auto) {
  // TODO: Assume here that we can propagate non-const?
  static_assert(
    std::is_reference_v<
      decltype(variant_traits<std::remove_cvref_t<V>>::template get<I>(v))>);
  return transfer_const_ref<V>(
    variant_traits<std::remove_cvref_t<V>>::template get<I>(v));
}

template <class V, class F>
constexpr auto match_one(V&& v, F&& f) -> decltype(auto) {
  using traits = variant_traits<std::remove_cvref_t<V>>;
  using return_type = std::invoke_result_t<F, decltype(get_impl<0>(v))>;
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
            std::forward<F>(f), get_impl<Is>(std::forward<V>(v))));
          static_assert(std::same_as<local_return_type, return_type>,
                        "all cases must have the same return type");
          return std::invoke(std::forward<F>(f),
                             get_impl<Is>(std::forward<V>(v)));
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

template <class V, class... Fs>
constexpr auto match(V&& v, Fs&&... fs) -> decltype(auto) {
  // We materialize a `detail::overload` below, which means copying and moving
  // the functions.
  if constexpr (caf::detail::is_specialization<std::tuple,
                                               std::remove_cvref_t<V>>::value) {
    return match_tuple(std::forward<V>(v),
                       detail::overload{std::forward<Fs>(fs)...});
  } else {
    return match_one(std::forward<V>(v),
                     detail::overload{std::forward<Fs>(fs)...});
  }
}

template <class V, class T>
constexpr auto type_to_variant_index = std::invoke(
  []<size_t... Is>(std::index_sequence<Is...>) {
    auto result = 0;
    auto found
      = (std::invoke([&] {
           if (std::same_as<
                 std::decay_t<decltype(variant_traits<V>::template get<Is>(
                   std::declval<V>()))>,
                 T>) {
             result = Is;
             return 1;
           }
           return 0;
         })
         + ... + 0);
    if (found == 0) {
      throw std::runtime_error{"type was not found in variant"};
    }
    if (found > 1) {
      throw std::runtime_error{"type was found multiple times in variant"};
    }
    return result;
  },
  std::make_index_sequence<variant_traits<V>::count>());

// TODO: Figure out how the function should be called. We could also use a
// functor here to disable ADL, but for `get` we can't have our other overloads.
// template <size_t I>
// constexpr auto get = []<class V>(V&& v) -> decltype(auto) {
//   TENZIR_ASSERT(variant_traits<V>::index(v) == I);
//   return get_impl<I>(std::forward<V>(v));
// };
template <class T, class V>
auto cast(V&& v) -> decltype(auto) {
  constexpr auto index = type_to_variant_index<std::remove_cvref_t<V>, T>;
  TENZIR_ASSERT(variant_traits<std::remove_cvref_t<V>>::index(v) == index);
  return get_impl<index>(std::forward<V>(v));
};

// TODO: Should this really take a pointer? Maybe depends on chosen name.
template <class T, class V>
auto cast_if(V* v) -> T* {
  constexpr auto index = type_to_variant_index<V, T>;
  if (not v) {
    return nullptr;
  }
  if (variant_traits<std::remove_const_t<V>>::index(*v) != index) {
    return nullptr;
  }
  return &get_impl<index>(*v);
};

// TODO: There are multiple ways to write down `match`. We should choose a
// syntax that works with our formatting rules.
template <class T, class... Fs>
auto match2(T&& x) {
  return [](auto...) {
    return 42;
  };
}
template <class... Fs>
auto match3(Fs&&...) {
  return [](auto...) {
    return 42;
  };
}
template <class... Fs>
class [[nodiscard]] match4 : Fs... {
public:
  explicit match4(Fs&&... fs) : Fs{std::forward<Fs>(fs)}... {
  }

  template <class... Variants>
  auto operator()(Variants&&... variants) -> decltype(auto) {
    return 42;
  }
};

} // namespace tenzir
