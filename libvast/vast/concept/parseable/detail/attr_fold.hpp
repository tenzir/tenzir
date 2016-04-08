#ifndef VAST_CONCEPT_PARSEABLE_DETAIL_ATTR_FOLD_HPP
#define VAST_CONCEPT_PARSEABLE_DETAIL_ATTR_FOLD_HPP

#include <string>
#include <vector>
#include <type_traits>

namespace vast {

struct unused_type;

namespace detail {

template <typename Attribute>
struct attr_fold : std::decay<Attribute> {};

template <>
struct attr_fold<std::vector<char>> : std::decay<std::string> {};

template <>
struct attr_fold<unused_type> : std::decay<unused_type> {};

template <>
struct attr_fold<std::vector<unused_type>> : std::decay<unused_type> {};

template <>
struct attr_fold<std::tuple<char, char>> : std::decay<std::string> {};

template <>
struct attr_fold<std::tuple<char, std::string>> : std::decay<std::string> {};

template <>
struct attr_fold<std::tuple<std::string, char>> : std::decay<std::string> {};

} // namespace detail
} // namespace vast

#endif
