#ifndef VAST_UTIL_VARIANT_H
#define VAST_UTIL_VARIANT_H

#include "juice/variant.hpp"

namespace vast {
namespace util {

/// A discriminated union container.
template <typename Head, typename... Tail>
using variant = Juice::Variant<Head, Tail...>;

template <typename T>
using recursive_wrapper = Juice::recursive_wrapper<T>;

using Juice::apply_visitor;
using Juice::apply_visitor_binary;

template <typename T, typename Head, typename... Tail>
auto get(variant<Head, Tail...>& v)
  -> decltype(Juice::get<T>(&v))
{
  return Juice::get<T>(&v);
}

template <typename T, typename Head, typename... Tail>
auto get(variant<Head, Tail...> const& v)
  -> decltype(Juice::get<T const>(&v))
{
  return Juice::get<T const>(&v);
}

} // namespace util
} // namespace vast

#endif
