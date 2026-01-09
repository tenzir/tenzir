//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/config.hpp>
#include <caf/detail/type_list.hpp>

namespace tenzir::detail {

#if CAF_MAJOR_VERSION >= 1
using caf::type_list;
#else
using caf::detail::type_list;
#endif
using caf::detail::empty_type_list;
using caf::detail::is_type_list;
using caf::detail::tl_apply;
using caf::detail::tl_at;
using caf::detail::tl_back;
using caf::detail::tl_contains;
using caf::detail::tl_exists;
using caf::detail::tl_filter;
using caf::detail::tl_filter_not;
using caf::detail::tl_forall;
using caf::detail::tl_head;
using caf::detail::tl_index_of;
using caf::detail::tl_is_distinct;
using caf::detail::tl_size;

template <class List>
struct tl_empty {
  static constexpr bool value = std::is_same<empty_type_list, List>::value;
};

template <class List>
using tl_head_t = typename tl_head<List>::type;

template <class List>
struct tl_tail;

template <>
struct tl_tail<type_list<>> {
  using type = type_list<>;
};

template <typename T0, class... Ts>
struct tl_tail<type_list<T0, Ts...>> {
  using type = type_list<Ts...>;
};

template <class List>
using tl_tail_t = typename tl_tail<List>::type;

template <class List>
using tl_back_t = typename tl_back<List>::type;

template <size_t LeftOffset, size_t Remaining, typename PadType, class List,
          class... Ts>
struct tl_slice_impl {
  using type = typename tl_slice_impl<LeftOffset - 1, Remaining, PadType,
                                      tl_tail_t<List>, Ts...>::type;
};

template <size_t Remaining, typename PadType, class List, class... Ts>
struct tl_slice_impl<0, Remaining, PadType, List, Ts...> {
  using type =
    typename tl_slice_impl<0, Remaining - 1, PadType, tl_tail_t<List>, Ts...,
                           tl_head_t<List>>::type;
};

template <size_t Remaining, typename PadType, class... Ts>
struct tl_slice_impl<0, Remaining, PadType, empty_type_list, Ts...> {
  using type = typename tl_slice_impl<0, Remaining - 1, PadType,
                                      empty_type_list, Ts..., PadType>::type;
};

template <class PadType, class List, class... T>
struct tl_slice_impl<0, 0, PadType, List, T...> {
  using type = type_list<T...>;
};

template <class PadType, class... T>
struct tl_slice_impl<0, 0, PadType, empty_type_list, T...> {
  using type = type_list<T...>;
};

template <class List, size_t ListSize, size_t First, size_t Last,
          typename PadType = caf::unit_t>
struct tl_slice_ {
  using type =
    typename tl_slice_impl<First, (Last - First), PadType, List>::type;
};

template <class List, size_t ListSize, typename PadType>
struct tl_slice_<List, ListSize, 0, ListSize, PadType> {
  using type = List;
};

/// Creates a new list from range (First, Last].
template <class List, size_t First, size_t Last>
struct tl_slice {
  using type = typename tl_slice_<List, tl_size<List>::value,
                                  (First > Last ? Last : First), Last>::type;
};

template <class List, size_t First, size_t Last>
using tl_slice_t = typename tl_slice<List, First, Last>::type;

template <class List, size_t First, size_t Last>
using tl_slice_t = typename tl_slice<List, First, Last>::type;

template <class From, class... Elements>
struct tl_reverse_impl;

template <class T0, class... T, class... E>
struct tl_reverse_impl<type_list<T0, T...>, E...> {
  using type = typename tl_reverse_impl<type_list<T...>, T0, E...>::type;
};

template <class... E>
struct tl_reverse_impl<empty_type_list, E...> {
  using type = type_list<E...>;
};

/// Creates a new list with elements in reversed order.
template <class List>
struct tl_reverse {
  using type = typename tl_reverse_impl<List>::type;
};

template <class List>
using tl_reverse_t = typename tl_reverse<List>::type;

template <class List, class T>
constexpr auto tl_contains_v = tl_contains<List, T>::value;

template <class ListA, class ListB>
struct tl_concat_impl;

/// Concatenates two lists.
template <class... LhsTs, class... RhsTs>
struct tl_concat_impl<type_list<LhsTs...>, type_list<RhsTs...>> {
  using type = type_list<LhsTs..., RhsTs...>;
};

// static list concat(list, list)

/// Concatenates lists.
template <class... Lists>
struct tl_concat;

template <class List0>
struct tl_concat<List0> {
  using type = List0;
};

template <class List0, class List1, class... Lists>
struct tl_concat<List0, List1, Lists...> {
  using type = typename tl_concat<typename tl_concat_impl<List0, List1>::type,
                                  Lists...>::type;
};

template <class... Lists>
using tl_concat_t = typename tl_concat<Lists...>::type;

template <class List, class What>
struct tl_push_back;

/// Appends `What` to given list.
template <class... ListTs, class What>
struct tl_push_back<type_list<ListTs...>, What> {
  using type = type_list<ListTs..., What>;
};

template <class List, class What>
using tl_push_back_t = typename tl_push_back<List, What>::type;

template <class T, template <class> class... Funs>
struct tl_apply_all;

template <class T>
struct tl_apply_all<T> {
  using type = T;
};

template <class T, template <class> class Fun0, template <class> class... Funs>
struct tl_apply_all<T, Fun0, Funs...> {
  using type = typename tl_apply_all<typename Fun0<T>::type, Funs...>::type;
};

template <class T, template <class> class... Funs>
using tl_apply_all_t = typename tl_apply_all<T, Funs...>::type;

/// Creates a new list by applying a "template function" to each element.
template <class List, template <class> class... Funs>
struct tl_map;

template <class... Ts, template <class> class... Funs>
struct tl_map<type_list<Ts...>, Funs...> {
  using type = type_list<tl_apply_all_t<Ts, Funs...>...>;
};

template <class List, template <class> class... Funs>
using tl_map_t = typename tl_map<List, Funs...>::type;

template <class List, size_t N>
using tl_at_t = typename tl_at<List, N>::type;

template <class List, class What>
struct tl_prepend;

/// Creates a new list with `What` prepended to `List`.
template <class What, class... T>
struct tl_prepend<type_list<T...>, What> {
  using type = type_list<What, T...>;
};

template <class List, class What>
using tl_prepend_t = typename tl_prepend<List, What>::type;

template <class List, bool... Selected>
struct tl_filter_impl;

template <>
struct tl_filter_impl<empty_type_list> {
  using type = empty_type_list;
};

template <class T0, class... T, bool... S>
struct tl_filter_impl<type_list<T0, T...>, false, S...> {
  using type = typename tl_filter_impl<type_list<T...>, S...>::type;
};

template <class T0, class... T, bool... S>
struct tl_filter_impl<type_list<T0, T...>, true, S...> {
  using type
    = tl_prepend_t<typename tl_filter_impl<type_list<T...>, S...>::type, T0>;
};

template <class List, template <class> class Pred>
using tl_filter_t = typename tl_filter<List, Pred>::type;

template <class List, template <class> class Pred>
using tl_filter_not_t = typename tl_filter_not<List, Pred>::type;

template <class List, class Type>
struct tl_filter_type;

template <class Type, class... T>
struct tl_filter_type<type_list<T...>, Type> {
  using type = typename tl_filter_impl<type_list<T...>,
                                       !std::is_same<T, Type>::value...>::type;
};

template <class List, class T>
using tl_filter_type_t = typename tl_filter_type<List, T>::type;

/// Creates a new list containing all elements which
///    are not equal to `Type`.
template <class List, class Type>
struct tl_filter_not_type;

template <class Type, class... T>
struct tl_filter_not_type<type_list<T...>, Type> {
  using type =
    typename tl_filter_impl<type_list<T...>,
                            (!std::is_same<T, Type>::value)...>::type;
};

template <class List, class T>
using tl_filter_not_type_t = typename tl_filter_not_type<List, T>::type;

/// Creates a new list from `List` without any duplicate elements.
template <class List>
struct tl_distinct;

template <>
struct tl_distinct<empty_type_list> {
  using type = empty_type_list;
};

template <class T0, class... Ts>
struct tl_distinct<type_list<T0, Ts...>> {
  using type = tl_concat_t<
    type_list<T0>,
    typename tl_distinct<tl_filter_type_t<type_list<Ts...>, T0>>::type>;
};

template <class List>
using tl_distinct_t = typename tl_distinct<List>::type;

template <class List, template <class...> class VarArgTemplate>
using tl_apply_t = typename tl_apply<List, VarArgTemplate>::type;

// Construct a type list from types that take a list of variadic template
// arguments.

template <class T>
struct tl_make;

template <template <class...> class T, class... Ts>
struct tl_make<T<Ts...>> {
  using type = type_list<Ts...>;
};

template <class T>
using tl_make_t = typename tl_make<T>::type;

template <class...>
struct common_types_helper;

template <class L1>
struct common_types_helper<L1, type_list<>> {
  using type = type_list<>;
};

template <class L1, class L2>
struct common_types_helper<L1, L2> {
  using type = std::conditional_t<
    tl_contains_v<L1, tl_head_t<L2>>,
    tl_prepend_t<typename common_types_helper<L1, detail::tl_tail_t<L2>>::type,
                 tl_head_t<L2>>,
    typename common_types_helper<L1, detail::tl_tail_t<L2>>::type>;
};

// Creates a new type list that contains all the types that are present in both
// L1 and L2
template <class L1, class L2>
  requires(is_type_list<L1>::value and is_type_list<L2>::value)
using tl_common_types_t
  = tl_distinct_t<typename common_types_helper<L1, L2>::type>;

} // namespace tenzir::detail
