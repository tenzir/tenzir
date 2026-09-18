//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concepts.hpp"

#include <algorithm>
#include <array>
#include <cstddef>

namespace tenzir::nova {

template <typename... Ts>
struct TypeList;

namespace type_list_detail {
template <concepts::instantiation_of<TypeList> L, typename... Rs>
struct JoinLists;

template <typename... L, typename... R>
struct JoinLists<TypeList<L...>, R...>
  : std::type_identity<TypeList<L..., R...>> {};

template <typename... L, typename... R>
struct JoinLists<TypeList<L...>, TypeList<R...>>
  : std::type_identity<TypeList<L..., R...>> {};

template <typename... Ts>
struct FlattenLists;

template <>
struct FlattenLists<> : std::type_identity<TypeList<>> {};

template <typename Head, typename... Rest>
struct FlattenLists<Head, Rest...>
  : std::type_identity<typename JoinLists<
      TypeList<Head>, typename FlattenLists<Rest...>::type>::type> {};

template <typename... Ts, typename... Rest>
struct FlattenLists<TypeList<Ts...>, Rest...>
  : std::type_identity<typename JoinLists<
      TypeList<Ts...>, typename FlattenLists<Rest...>::type>::type> {};

template <typename T, typename... Ts>
struct tuple_index;

template <typename T, typename... Ts>
consteval auto unique_index_of() noexcept -> std::size_t {
  constexpr static std::array found = {std::same_as<T, Ts>...};
  static_assert(std::ranges::count(found, true) == 1,
                "T must be unique in type list");
  return static_cast<std::size_t>(std::ranges::find(found, true)
                                  - found.begin());
}
} // namespace type_list_detail

template <typename... Ts>
struct TypeList {
  constexpr static auto size = sizeof...(Ts);

  template <typename T>
  constexpr static bool contains = concepts::one_of<T, Ts...>;

  template <typename T>
  constexpr static auto unique_index_of
    = type_list_detail::unique_index_of<T, Ts...>();

  template <size_t I>
    requires(I < size)
  using at = std::tuple_element_t<I, std::tuple<Ts...>>;

  template <template <typename...> class C>
  using apply = C<Ts...>;

  template <template <typename> class T>
  using wrap = TypeList<T<Ts>...>;

  template <template <typename> class T>
  using transform = TypeList<typename T<Ts>::type...>;

  template <template <typename> class T>
  constexpr static bool all_of = (T<Ts>::value && ...);

  template <typename V, template <typename, typename> class Pred>
  constexpr static bool one_of = (Pred<V, Ts>::value || ...);

  template <typename... Rs>
  using join =
    typename type_list_detail::JoinLists<TypeList<Ts...>, Rs...>::type;

  template <typename T>
  using append = join<T>;

  using flatten = typename type_list_detail::FlattenLists<Ts...>::type;

  constexpr static auto index_sequence = std::index_sequence_for<Ts...>{};
};

} // namespace tenzir::nova
