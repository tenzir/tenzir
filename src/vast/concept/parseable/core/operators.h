#ifndef VAST_CONCEPT_PARSEABLE_CORE_OPERATORS_H
#define VAST_CONCEPT_PARSEABLE_CORE_OPERATORS_H

#include <type_traits>

#include "vast/concept/parseable/detail/as_parser.h"

namespace vast {

template <typename>
class and_parser;

template <typename>
class not_parser;

template <typename>
class optional_parser;

template <typename>
class kleene_parser;

template <typename>
class plus_parser;

template <typename, typename>
class difference_parser;

template <typename, typename>
class list_parser;

template <typename, typename>
class sequence_parser;

template <typename, typename>
class choice_parser;

//
// Unary
//

template <typename T>
auto operator&(T x)
  -> std::enable_if_t<is_parser<T>{}, and_parser<T>>
{
  return and_parser<T>{x};
}

template <typename T>
auto operator!(T x)
  -> std::enable_if_t<is_parser<T>{}, not_parser<T>>
{
  return not_parser<T>{x};
}

template <typename T>
auto operator-(T x)
  -> std::enable_if_t<is_parser<T>{}, optional_parser<T>>
{
  return optional_parser<T>{x};
}

template <typename T>
auto operator*(T x)
  -> std::enable_if_t<is_parser<T>{}, kleene_parser<T>>
{
  return kleene_parser<T>{x};
}

template <typename T>
auto operator+(T x)
  -> std::enable_if_t<is_parser<T>{}, plus_parser<T>>
{
  return plus_parser<T>{x};
}

//
// Binary
//

template <typename LHS, typename RHS>
auto operator-(LHS&& lhs, RHS&& rhs)
  -> decltype(detail::as_parser<difference_parser>(lhs, rhs))
{
  return {detail::as_parser(std::forward<LHS>(lhs)),
          detail::as_parser(std::forward<RHS>(rhs))};
}

template <typename LHS, typename RHS>
auto operator%(LHS&& lhs, RHS&& rhs)
  -> decltype(detail::as_parser<list_parser>(lhs, rhs))
{
  return {detail::as_parser(std::forward<LHS>(lhs)),
          detail::as_parser(std::forward<RHS>(rhs))};
}

template <typename LHS, typename RHS>
auto operator>>(LHS&& lhs, RHS&& rhs)
  -> decltype(detail::as_parser<sequence_parser>(lhs, rhs))
{
  return {detail::as_parser(std::forward<LHS>(lhs)),
          detail::as_parser(std::forward<RHS>(rhs))};
}

template <typename LHS, typename RHS>
auto operator|(LHS&& lhs, RHS&& rhs)
  -> decltype(detail::as_parser<choice_parser>(lhs, rhs))
{
  return {detail::as_parser(std::forward<LHS>(lhs)),
          detail::as_parser(std::forward<RHS>(rhs))};
}

} // namespace vast

#endif
