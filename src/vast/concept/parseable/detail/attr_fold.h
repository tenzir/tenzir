#ifndef VAST_CONCEPT_PARSEABLE_DETAIL_ATTR_FOLD_H
#define VAST_CONCEPT_PARSEABLE_DETAIL_ATTR_FOLD_H

#include <string>
#include <vector>
#include <type_traits>

namespace vast {

struct unused_type;

namespace detail {

template <typename Attribute>
struct attr_fold : std::common_type<Attribute> {};

template <>
struct attr_fold<std::vector<char>> : std::common_type<std::string> {};

template <>
struct attr_fold<unused_type> : std::common_type<unused_type> {};

} // namespace detail
} // namespace vast

#endif
