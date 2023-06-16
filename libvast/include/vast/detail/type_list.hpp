//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/detail/is_one_of.hpp>
#include <caf/detail/type_list.hpp>

#include <tuple>

namespace vast::detail {

using caf::detail::tbind;

using caf::detail::empty_type_list;
using caf::detail::is_type_list;
using caf::detail::tl_apply;
using caf::detail::tl_apply_all;
using caf::detail::tl_at;
using caf::detail::tl_back;
using caf::detail::tl_concat;
using caf::detail::tl_count;
using caf::detail::tl_count_not;
using caf::detail::tl_distinct;
using caf::detail::tl_empty;
using caf::detail::tl_exists;
using caf::detail::tl_filter;
using caf::detail::tl_filter_not;
using caf::detail::tl_find;
using caf::detail::tl_forall;
using caf::detail::tl_head;
using caf::detail::tl_index_of;
using caf::detail::tl_is_distinct;
using caf::detail::tl_map;
using caf::detail::tl_map_conditional;
using caf::detail::tl_pop_back;
using caf::detail::tl_prepend;
using caf::detail::tl_push_back;
using caf::detail::tl_push_front;
using caf::detail::tl_reverse;
using caf::detail::tl_size;
using caf::detail::tl_slice;
using caf::detail::tl_tail;
using caf::detail::tl_trim;
using caf::detail::tl_unzip;
using caf::detail::tl_zip;
using caf::detail::type_list;
// using caf::detail::tl_is_strict_subset;
using caf::detail::tl_equal;

template <class List>
using tl_head_t = typename tl_head<List>::type;

template <class List>
using tl_tail_t = typename tl_tail<List>::type;

template <class List>
using tl_back_t = typename tl_back<List>::type;

template <class List, size_t First, size_t Last>
using tl_slice_t = typename tl_slice<List, First, Last>::type;

template <class ListA, class ListB, template <class, class> class Fun>
using tl_zip_t = typename tl_zip<ListA, ListB, Fun>::type;

template <class List>
using tl_unzip_t = typename tl_unzip<List>::type;

template <class List>
using tl_reverse_t = typename tl_reverse<List>::type;

template <class ListA, class ListB>
using tl_concat_t = typename tl_concat<ListA, ListB>::type;

template <class List, class What>
using tl_push_back_t = typename tl_push_back<List, What>::type;

template <class List, class What>
using tl_push_front_t = typename tl_push_front<List, What>::type;

template <class L, template <class> class... Funs>
using tl_apply_all_t = typename tl_apply_all<L, Funs...>::type;

template <class List, template <class> class... Funs>
using tl_map_t = typename tl_map<List, Funs...>::type;

template <class List, template <class> class Trait, bool TRes,
          template <class> class... Funs>
using tl_map_conditional_t =
  typename tl_map_conditional<List, Trait, TRes, Funs...>::type;

template <class List>
using tl_popback_t = typename tl_pop_back<List>::type;

template <class List, size_t N>
using tl_at_t = typename tl_at<List, N>::type;

template <class List, class What>
using tl_prepend_t = typename tl_prepend<List, What>::type;

template <class List, template <class> class Pred>
using tl_filter_t = typename tl_filter<List, Pred>::type;

template <class List, template <class> class Pred>
using tl_filter_not_t = typename tl_filter_not<List, Pred>::type;

template <class List>
using tl_distinct_t = typename tl_distinct<List>::type;

template <class List, class What>
using tl_trim_t = typename tl_trim<List, What>::type;

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
    caf::detail::tl_contains<L1, caf::detail::tl_head_t<L2>>::value,
    tl_prepend_t<
      typename common_types_helper<L1, caf::detail::tl_tail_t<L2>>::type,
      caf::detail::tl_head_t<L2>>,
    typename common_types_helper<L1, caf::detail::tl_tail_t<L2>>::type>;
};

// Creates a new type list that contains all the types that are present in both
// L1 and L2
template <class L1, class L2>
  requires(is_type_list<L1>::value and is_type_list<L2>::value)
using tl_common_types_t
  = tl_distinct_t<typename common_types_helper<L1, L2>::type>;

} // namespace vast::detail
