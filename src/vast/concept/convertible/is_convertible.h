#ifndef VAST_CONCEPT_CONVERTIBLE_IS_CONVERTIBLE_H
#define VAST_CONCEPT_CONVERTIBLE_IS_CONVERTIBLE_H

#include <type_traits>

namespace vast {
namespace detail {

struct is_convertible {
  template <typename From, typename To>
  static auto test(From const* from, To* to)
    -> decltype(convert(*from, *to), std::true_type());

  template <typename, typename>
  static auto test(...) -> std::false_type;
};

} // namespace detail

/// Type trait that checks whether a type is convertible to another.
template <typename From, typename To>
struct is_convertible
  : decltype(detail::is_convertible::test<std::decay_t<From>, To>(0, 0)) {};

} // namespace vast

#endif
