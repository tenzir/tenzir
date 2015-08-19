#ifndef VAST_UTIL_TYPE_LIST_H
#define VAST_UTIL_TYPE_LIST_H

#include <caf/detail/type_list.hpp>

namespace vast {
namespace util {

using caf::detail::type_list;
using caf::detail::empty_type_list;
using caf::detail::is_type_list;
using caf::detail::tl_head;
using caf::detail::tl_tail;
using caf::detail::tl_size;
using caf::detail::tl_back;
using caf::detail::tl_empty;
using caf::detail::tl_slice;
using caf::detail::tl_zip;
using caf::detail::tl_unzip;
using caf::detail::tl_index_of;
using caf::detail::tl_reverse;
using caf::detail::tl_find;
using caf::detail::tl_find_if;
using caf::detail::tl_forall;
using caf::detail::tl_exists;
using caf::detail::tl_count;
using caf::detail::tl_count_not;
using caf::detail::tl_concat;
using caf::detail::tl_push_back;
using caf::detail::tl_push_front;
using caf::detail::tl_apply_all;
using caf::detail::tl_map;
using caf::detail::tl_map_conditional;
using caf::detail::tl_pop_back;
using caf::detail::tl_at;
using caf::detail::tl_prepend;
using caf::detail::tl_filter;
using caf::detail::tl_filter_not;
using caf::detail::tl_distinct;
using caf::detail::tl_is_distinct;
using caf::detail::tl_trim;
using caf::detail::tl_apply;
// using caf::detail::tl_is_strict_subset;
using caf::detail::tl_equal;

template <typename List>
using tl_head_t = typename tl_head<List>::type;

template <typename List>
using tl_tail_t = typename tl_tail<List>::type;

template <typename List>
using tl_back_t = typename tl_back<List>::type;

template <typename List, size_t First, size_t Last>
using tl_slice_t = typename tl_slice<List, First, Last>::type;

template <class ListA, class ListB, template <class, class> class Fun>
using tl_zip_t = typename tl_zip<ListA, ListB, Fun>::type;

template <typename List>
using tl_unzip_t = typename tl_unzip<List>::type;

template <typename List>
using tl_reverse_t = typename tl_reverse<List>::type;

template <typename ListA, typename ListB>
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

template <typename List>
using tl_popback_t = typename tl_pop_back<List>::type;

template <typename List, size_t N>
using tl_at_t = typename tl_at<List, N>::type;

template <class List, class What>
using tl_prepend_t = typename tl_prepend<List, What>::type;

template <class List, template <class> class Pred>
using tl_filter_t = typename tl_filter<List, Pred>::type;

template <class List, template <class> class Pred>
using tl_filter_not_t = typename tl_filter_not<List, Pred>::type;

template <typename List>
using tl_distinct_t = typename tl_distinct<List>::type;

template <class List, class What>
using tl_trim_t = typename tl_trim<List, What>::type;

template <class List, template <class...> class VarArgTemplate>
using tl_apply_t = typename tl_apply<List, VarArgTemplate>::type;

} // namspace util
} // namspace vast

#endif
