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
using caf::detail::tl_is_strict_subset;
using caf::detail::tl_equal;

} // namspace util
} // namspace vast

#endif
